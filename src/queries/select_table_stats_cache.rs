use super::{
    select_table_stats::{Constraint, TableColumnStat, TableIndex, TableReferencedBy, TableStats},
    utils::validate_table_name,
};
use crate::Error;
use futures::stream::Stream;
use futures03::{compat::Future01CompatExt, future::try_join3};
use lazy_static::lazy_static;
use std::collections::HashMap;

use tokio_postgres::{impls::Prepare, row::Row, Client};

/// Returns the requested tables’ stats: number of rows, the foreign keys referring to the table,
/// and column names + types
pub async fn select_all_table_stats(
    mut conn: Client,
    tables: Vec<String>,
) -> Result<HashMap<String, TableStats>, Error> {
    let tables_str: String = match tables
        .iter()
        .map(|table| -> Result<String, Error> {
            validate_table_name(table)?;
            Ok(["'", table, "'"].join(""))
        })
        .collect::<Result<Vec<String>, Error>>()
    {
        Ok(quoted_tables) => quoted_tables.join(", "),
        Err(e) => return Err(e),
    };

    let (constraints_statement, indexes_statement, column_stats_statement) = match try_join3(
        select_constraints_statement(&mut conn, &tables_str, &tables).compat(),
        select_indexes_statement(&mut conn, &tables_str).compat(),
        select_column_stats_statement(&mut conn, &tables_str).compat(),
    )
    .await
    {
        Ok((constraints_statement, indexes_statement, column_stats_statement)) => (
            constraints_statement,
            indexes_statement,
            column_stats_statement,
        ),
        Err(e) => return Err(Error::from(e)),
    };

    let (constraints, indexes, column_stats) = match try_join3(
        conn.query(&constraints_statement, &[]).collect().compat(),
        conn.query(&indexes_statement, &[]).collect().compat(),
        conn.query(&column_stats_statement, &[]).collect().compat(),
    )
    .await
    {
        Ok((constraint_rows, index_rows, stat_rows)) => (constraint_rows, index_rows, stat_rows),
        Err(e) => return Err(Error::from(e)),
    };

    Ok(compile_table_stats(
        tables,
        constraints,
        indexes,
        column_stats,
    ))
}

/// Takes the results of individual queries and generates the final table stats object
fn compile_table_stats(
    tables: Vec<String>,
    constraint_rows: Vec<Row>,
    index_rows: Vec<Row>,
    column_stat_rows: Vec<Row>,
) -> HashMap<String, TableStats> {
    let mut constraints = process_constraints(constraint_rows);
    let mut indexes = process_indexes(index_rows);
    let mut column_stats = process_column_stats(column_stat_rows);

    let mut table_stats: HashMap<String, TableStats> = HashMap::new();

    for table in tables.into_iter() {
        let table_constraints = match constraints.remove_entry(&table) {
            Some((_t, constraint_vec)) => constraint_vec,
            None => vec![],
        };
        let table_indexes = match indexes.remove_entry(&table) {
            Some((_t, index_vec)) => index_vec,
            None => vec![],
        };
        let table_column_stats = match column_stats.remove_entry(&table) {
            Some((_t, stat_vec)) => stat_vec,
            None => vec![],
        };

        // calculate primary key + referenced_by by iterating constraints and trimming the pK_column
        let mut opt_primary_key = vec![];
        let mut referenced_by = vec![];
        for constraint in &table_constraints {
            match constraint.constraint_type {
                "foreign_key" => {
                    if let Some(fk_table) = &constraint.fk_table {
                        // push foreign key information to referenced_by if fk_table matches the
                        // current table
                        if fk_table == &table {
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

        table_stats.insert(
            table,
            TableStats {
                columns: table_column_stats,
                constraints: table_constraints,
                indexes: table_indexes,
                primary_key: match opt_primary_key.len() {
                    0 => None,
                    _ => Some(opt_primary_key),
                },
                referenced_by,
            },
        );
    }

    table_stats
}

fn select_column_stats_statement(conn: &mut Client, tables_str: &str) -> Prepare {
    let statement_str = &format!("
WITH foreign_keys as (
    SELECT
        tbl.relname AS table_name,
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
        (tbl.relname IN ({0}))
    )
    GROUP BY c.oid, table_name, column_name
    ORDER BY table_name, column_name
)
SELECT
    c.table_name,
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
    LEFT JOIN foreign_keys f ON c.column_name = f.column_name AND c.table_name = f.table_name
WHERE
    table_schema = 'public' AND
    c.table_name IN ({0})
ORDER BY c.table_name, column_name;", tables_str);

    conn.prepare(&statement_str)
}

/// Returns a given tables’ column stats: column names, column types, length, default values, and
/// foreign keys information.
fn process_column_stats(rows: Vec<Row>) -> HashMap<String, Vec<TableColumnStat>> {
    let mut table_column_stats: HashMap<String, Vec<TableColumnStat>> = HashMap::new();
    for row in rows {
        let table: String = row.get(0);
        let is_nullable_string: String = row.get(6);

        let column_stats = TableColumnStat {
            column_name: row.get(1),
            column_type: row.get(2),
            default_value: row.get(3),
            is_nullable: match is_nullable_string.as_str() {
                "YES" => true,
                "NO" => false,
                _ => false,
            },
            is_foreign_key: row.get(7),
            foreign_key_table: row.get(8),
            foreign_key_column: row.get(9),
            char_max_length: row.get(4),
            char_octet_length: row.get(5),
        };

        table_column_stats
            .entry(table.clone())
            .or_insert_with(|| vec![]);
        table_column_stats
            .get_mut(&table)
            .unwrap()
            .push(column_stats);
    }

    table_column_stats
}

fn select_constraints_statement(conn: &mut Client, tables_str: &str, tables: &[String]) -> Prepare {
    let mut statement_str_vec = vec![
        r#"
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
    (tbl.relname IN ("#,
    tables_str,
    ") OR (",
    ];

    let tables_where_vec = &tables
        .iter()
        .map(|table| ["pg_get_constraintdef(c.oid) LIKE '%", table, "(%'"])
        .collect::<Vec<[&str; 3]>>();
    for (i, tables_where_arr) in tables_where_vec.iter().enumerate() {
        statement_str_vec.extend(tables_where_arr);
        if i < tables_where_vec.len() - 1 {
            statement_str_vec.push(" OR ");
        }
    }

    statement_str_vec.push(
        r#")
))
GROUP BY name, constraint_type, "table", definition
ORDER BY "table";"#,
    );
    let statement_str = statement_str_vec.join("");

    conn.prepare(&statement_str)
}

fn process_constraints(rows: Vec<Row>) -> HashMap<String, Vec<Constraint>> {
    // retrieves all constraints (c = check, u = unique, f = foreign key, p = primary key)
    // shamelessly taken from https://dba.stackexchange.com/questions/36979/retrieving-all-pk-and-fk

    // Using lazy_static so that COLUMN_REG and CONSTRAINT_MAP are only compiled once total (versus
    // compiling every time this function is called)
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

    let mut table_constraints: HashMap<String, Vec<Constraint>> = HashMap::new();
    for row in rows {
        let constraint_type_int: i8 = row.get(1);
        let constraint_type_uint: u8 = constraint_type_int as u8;
        let constraint_type_char: char = constraint_type_uint.into();
        let table: String = row.get(2);

        let constraint = Constraint {
            name: row.get(0),
            table: table.clone(),
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
        };

        table_constraints
            .entry(table.clone())
            .or_insert_with(|| vec![]);
        table_constraints.get_mut(&table).unwrap().push(constraint);
    }

    table_constraints
}

fn select_indexes_statement(conn: &mut Client, tables_str: &str) -> Prepare {
    // taken from https://stackoverflow.com/a/2213199
    let statement_str = [
        "
SELECT
    t.relname as table_name,
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
    and t.relname IN (",
        tables_str,
        ")
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
    ix.indisprimary;",
    ]
    .join("");

    conn.prepare(&statement_str)
}

// returns indexes (including primary keys) of a given table
fn process_indexes(rows: Vec<Row>) -> HashMap<String, Vec<TableIndex>> {
    let mut table_indexes: HashMap<String, Vec<TableIndex>> = HashMap::new();
    for row in rows {
        let table: String = row.get(0);
        let column_names_str: String = row.get(3);
        let index = TableIndex {
            name: row.get(1),
            columns: column_names_str
                .split(',')
                .map(std::string::ToString::to_string)
                .collect(),
            access_method: row.get(2),
            is_exclusion: row.get(5),
            is_primary_key: row.get(6),
            is_unique: row.get(4),
        };

        table_indexes.entry(table.clone()).or_insert_with(|| vec![]);
        table_indexes.get_mut(&table).unwrap().push(index);
    }

    table_indexes
}
