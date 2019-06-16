use super::utils::validate_sql_name;
use crate::errors::ApiError;
use futures::future::{err, join_all, Either, Future};
use futures::stream::Stream;
use std::collections::HashMap;

use tokio_postgres::impls::{Prepare, Query};
use tokio_postgres::{Client, Error};
#[derive(Debug, Deserialize, Serialize)]
/// Stats for a single column of a table
pub struct TableColumnStat {
    /// name of column
    pub column_name: String,
    /// type of column
    pub column_type: String,
    /// default value of column
    pub default_value: Option<String>,
    /// if null can be a column value
    pub is_nullable: bool,
    /// whether the column is a foreign key referencing another table
    pub is_foreign_key: bool,
    /// table being referenced (if is_foreign_key)
    pub foreign_key_table: Option<String>,
    /// table column being referenced (if is_foreign_key)
    pub foreign_key_column: Option<String>,
    /// If data_type identifies a character or bit string type, the declared maximum length; null for all other data types or if no maximum length was declared.
    pub char_max_length: Option<i32>,
    /// If data_type identifies a character type, the maximum possible length in octets (bytes) of a datum; null for all other data types. The maximum octet length depends on the declared character maximum length (see above) and the server encoding.
    pub char_octet_length: Option<i32>,
}

#[derive(Debug, Serialize)]
/// Details about other tables’ foreign keys that are referencing the current table
pub struct TableReferencedBy {
    /// The table with a foreign key referencing the current table
    pub referencing_table: String,
    /// The column that is a foreign key referencing the current table
    pub referencing_columns: Vec<String>,
    /// the column of the current table being referenced by the foreign key
    pub columns_referenced: Vec<String>,
}

#[derive(Debug, Serialize)]
/// A single index on the table.
pub struct TableIndex {
    /// index name
    pub name: String,
    /// columns involved
    pub columns: Vec<String>,
    /// btree, hash, gin, etc.
    pub access_method: String,
    pub is_exclusion: bool,
    pub is_primary_key: bool,
    pub is_unique: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Constraint {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub constraint_type: &'static str,
    pub definition: String,
    pub fk_table: Option<String>,
    pub fk_columns: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
/// A table’s stats: columns, indexes, foreign + primary keys, number of rows.
pub struct TableStats {
    pub columns: Vec<TableColumnStat>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<TableIndex>,
    pub primary_key: Option<Vec<String>>,
    pub referenced_by: Vec<TableReferencedBy>,
    pub rows: i64,
}

/// Returns the requested table’s stats: number of rows, the foreign keys referring to the table, and column names + types
pub fn select_table_stats(
    mut client: Client,
    table: String,
) -> impl Future<Item = (TableStats, Client), Error = (ApiError, Client)> {
    if let Err(e) = validate_sql_name(&table) {
        return Either::A(err::<(TableStats, Client), (ApiError, Client)>((e, client)));
    }

    // run all sub-operations in "parallel"

    // create prepared statements
    let f = join_all(vec![
        select_row_count_statement(&mut client, &table),
        select_constraints_statement(&mut client, &table),
        select_indexes_statement(&mut client, &table),
        select_column_stats_statement(&mut client, &table),
    ])
    .then(move |result| match result {
        // query the statements
        Ok(statements) => {
            let mut queries = vec![];
            for statement in &statements {
                queries.push(client.query(statement, &[]))
            }
            Ok((queries, client))
        }
        Err(e) => Err((e, client)),
    })
    .then(move |result| match result {
        // compile the results of the sub-operations into final stats
        Ok((mut queries, client)) => {
            let column_stats_q = queries.pop().unwrap();
            let indexes_q = queries.pop().unwrap();
            let constraints_q = queries.pop().unwrap();
            let count_q = queries.pop().unwrap();

            let count_f = select_row_count(count_q);
            let constraints_f = select_constraints(constraints_q);
            let indexes_f = select_indexes(indexes_q);
            let column_stats_f = select_column_stats(column_stats_q);

            let compile_f = count_f
                .join4(constraints_f, indexes_f, column_stats_f)
                .then(move |result| match result {
                    Ok((row_count, constraints, indexes, column_stats)) => Ok((
                        compile_table_stats(&table, row_count, constraints, indexes, column_stats),
                        client,
                    )),
                    Err(e) => Err((e, client)),
                });

            Either::A(compile_f)
        }
        Err((e, client)) => Either::B(err((e, client))),
    })
    .map_err(|(e, client)| (ApiError::from(e), client));

    Either::B(f)
}

/// Returns a given table’s column stats: column names, column types, length, default values, and foreign keys information.
pub fn select_column_stats(q: Query) -> impl Future<Item = Vec<TableColumnStat>, Error = Error> {
    q.map(|row| {
        let is_nullable_string: String = row.get(5);

        TableColumnStat {
            column_name: row.get(0),
            column_type: row.get(1),
            default_value: row.get(2),
            is_nullable: match is_nullable_string.as_str() {
                "YES" => true,
                "NO" => false,
                _ => false,
            },
            is_foreign_key: row.get(6),
            foreign_key_table: row.get(7),
            foreign_key_column: row.get(8),
            char_max_length: row.get(3),
            char_octet_length: row.get(4),
        }
    })
    .collect()
    .then(move |result| match result {
        Ok(stats) => Ok(stats),
        Err(e) => Err(e),
    })
}

pub fn select_column_stats_statement(client: &mut Client, table: &str) -> Prepare {
    let statement_str = &format!("
WITH foreign_keys as (
    SELECT
        col.attname AS column_name,

        substring(
            pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1
        ) AS fk_table,

        substring(
            pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1
        ) AS fk_column

    FROM
        pg_constraint c
        JOIN LATERAL UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
        JOIN pg_class tbl ON tbl.oid = c.conrelid
        JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
        JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = u.attnum)
    WHERE (
        sch.nspname = 'public' AND
        c.contype = 'f' AND
        (tbl.relname = '{0}')
    )
    GROUP BY c.oid, col.attname
    ORDER BY column_name
)
SELECT
    c.column_name,
    c.udt_name as column_type,
    c.column_default as default_value,
    c.character_maximum_length,
    c.character_octet_length,
    c.is_nullable,
    EXISTS(SELECT column_name from foreign_keys WHERE column_name = c.column_name) AS is_foreign_key,
    f.fk_table,
    f.fk_column
FROM
    information_schema.columns c
    LEFT JOIN foreign_keys f ON c.column_name = f.column_name
WHERE
    table_schema = 'public' AND
    table_name = '{0}'
ORDER BY table_name, column_name;", table);

    client.prepare(&statement_str)
}

/// Takes the results of individual queries and generates the final table stats object
fn compile_table_stats(
    table: &str,
    row_count: i64,
    constraints: Vec<Constraint>,
    indexes: Vec<TableIndex>,
    column_stats: Vec<TableColumnStat>,
) -> TableStats {
    // calculate primary key + referenced_by by iterating constraints and trimming the pK_column
    let mut opt_primary_key = vec![];
    let mut referenced_by = vec![];
    for constraint in &constraints {
        match constraint.constraint_type {
            "foreign_key" => {
                if let Some(fk_table) = &constraint.fk_table {
                    // push foreign key information to referenced_by if fk_table matches the current table
                    if fk_table == table {
                        referenced_by.push(TableReferencedBy {
                            referencing_table: constraint.table.clone(),
                            referencing_columns: constraint.columns.clone(),
                            // columns_referenced: constraint.fk_columns.unwrap().clone(),
                            columns_referenced: match &constraint.fk_columns {
                                Some(fk_columns) => fk_columns.clone(),
                                None => vec![],
                            },
                        });
                    }
                }
            }
            "primary_key" => {
                // push primary key information to opt_primary_key
                opt_primary_key = constraint.columns.clone();
            }
            _ => (),
        }
    }

    TableStats {
        columns: column_stats,
        constraints,
        indexes,
        primary_key: match opt_primary_key.len() {
            0 => None,
            _ => Some(opt_primary_key),
        },
        referenced_by,
        rows: row_count,
    }
}

fn select_constraints(q: Query) -> impl Future<Item = Vec<Constraint>, Error = Error> {
    // retrieves all constraints (c = check, u = unique, f = foreign key, p = primary key)
    // shamelessly taken from https://dba.stackexchange.com/questions/36979/retrieving-all-pk-and-fk

    // Using lazy_static so that COLUMN_REG and CONSTRAINT_MAP are only compiled once total (versus compiling every time this function is called)
    lazy_static! {
        // static ref COLUMN_REG: Regex = Regex::new(r"^\{(.*)\}$").unwrap();
        static ref CONSTRAINT_MAP: HashMap<char, &'static str> = {
            let mut m = HashMap::new();
            m.insert('c', "check");
            m.insert('f', "foreign_key");
            m.insert('p', "primary_key");
            m.insert('u', "unique");
            m.insert('t', "trigger");
            m.insert('x', "exclusion");

            m
        };
    }

    q.map(|row| {
        let constraint_type_int: i8 = row.get(1);
        let constraint_type_uint: u8 = constraint_type_int as u8;
        let constraint_type_char: char = constraint_type_uint.into();

        Constraint {
            name: row.get(0),
            table: row.get(2),
            columns: row.get(3),
            constraint_type: match CONSTRAINT_MAP.get(&constraint_type_char) {
                Some(constraint_type) => constraint_type,
                None => panic!("Unhandled constraint type: {}", constraint_type_char),
            },
            definition: row.get(4),
            fk_table: row.get(5),
            fk_columns: {
                let pk_column_raw: Option<String> = row.get(6);

                match pk_column_raw {
                    Some(pk_column) => Some(
                        pk_column
                            .split(',')
                            .map(|column| column.trim().to_string())
                            .collect(),
                    ),
                    None => None,
                }
            },
        }
    })
    .collect()
}

fn select_constraints_statement(client: &mut Client, table: &str) -> Prepare {
    let statement_str = format!(r#"
SELECT
    c.conname                   AS name,
    c.contype                   AS constraint_type,
    tbl.relname                 AS "table",
    ARRAY_AGG(
        col.attname ORDER BY u.attposition
     )                          AS columns,
    pg_get_constraintdef(c.oid) AS definition,

    CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %'
    THEN substring(
        pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1
    ) END AS "fk_table",

    CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %'
    THEN substring(
        pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1
    ) END AS "fk_column"

FROM pg_constraint c
    JOIN LATERAL UNNEST(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
    JOIN pg_class tbl ON tbl.oid = c.conrelid
    JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
    JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = u.attnum)
WHERE (
    sch.nspname = 'public' AND
    (tbl.relname = '{0}' OR pg_get_constraintdef(c.oid) LIKE '%{0}(%')
)
GROUP BY name, constraint_type, "table", definition
ORDER BY "table";"#, table);

    client.prepare(&statement_str)
}

// returns indexes (including primary keys) of a given table
fn select_indexes(q: Query) -> impl Future<Item = Vec<TableIndex>, Error = Error> {
    q.map(|row| {
        let column_names_str: String = row.get(2);
        TableIndex {
            name: row.get(0),
            columns: column_names_str
                .split(',')
                .map(std::string::ToString::to_string)
                .collect(),
            access_method: row.get(1),
            is_exclusion: row.get(4),
            is_primary_key: row.get(5),
            is_unique: row.get(3),
        }
    })
    .collect()
    .then(move |result| match result {
        Ok(rows) => Ok(rows),
        Err(e) => Err(e),
    })
}

fn select_indexes_statement(client: &mut Client, table: &str) -> Prepare {
    // taken from https://stackoverflow.com/a/2213199
    let statement_str = format!(
        r#"
SELECT
    i.relname as name,
    am.amname as access_method,
    array_to_string(array_agg(a.attname), ',') as columns,
    ix.indisunique as is_unique,
    ix.indisexclusion as is_exclusion,
    ix.indisprimary as is_primary_key
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    pg_am am
WHERE
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and t.relname = '{}'
    and i.relam = am.oid
GROUP BY
    t.relname,
    i.relname,
    am.amname,
    ix.indisunique,
    ix.indisexclusion,
    ix.indisprimary
ORDER BY
    t.relname,
    i.relname,
    am.amname,
    ix.indisunique,
    ix.indisexclusion,
    ix.indisprimary;"#,
        table
    );

    client.prepare(&statement_str)
}

fn select_row_count(q: Query) -> impl Future<Item = i64, Error = Error> {
    q.map(|row| row.get(0))
        .collect()
        .then(move |result| match result {
            Ok(rows) => Ok(rows[0]),
            Err(e) => Err(e),
        })
}

fn select_row_count_statement(client: &mut Client, table: &str) -> Prepare {
    let statement_str = ["SELECT COUNT(*) FROM ", table, ";"].join("");

    client.prepare(&statement_str)
}
