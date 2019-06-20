use super::utils::validate_sql_name;
use crate::errors::ApiError;
use futures03::compat::Future01CompatExt;
use futures01::stream::Stream;
use std::collections::HashMap;
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
}

/// Returns the requested table’s stats: number of rows, the foreign keys referring to the table, and column names + types
pub async fn select_table_stats(
    mut client: Client,
    table: String,
) -> Result<(TableStats, Client), (ApiError, Client)> {
    if let Err(e) = validate_sql_name(&table) {
        return Err((e, client));
    }

    let (constraints, client) = match select_constraints(client, &table).await {
        Ok((constraints, client)) => (constraints, client),
        Err((e, client)) => return Err((ApiError::from(e), client)),
    };
    let (indexes, client) = match select_indexes(client, &table).await {
        Ok((indexes, client)) => (indexes, client),
        Err((e, client)) => return Err((ApiError::from(e), client)),
    };
    let (column_stats, client) = match select_column_stats(client, &table).await {
        Ok((column_stats, client)) => (column_stats, client),
        Err((e, client)) => return Err((ApiError::from(e), client)),
    };

    Ok((
        compile_table_stats(&table, constraints, indexes, column_stats),
        client,
    ))
}

/// Returns a given table’s column stats: column names, column types, length, default values, and foreign keys information.
pub async fn select_column_stats(
    mut client: Client,
    table: &str,
) -> Result<(Vec<TableColumnStat>, Client), (Error, Client)> {
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

    let statement = match client.prepare(&statement_str).compat().await {
        Ok(statement) => statement,
        Err(e) => return Err((e, client)),
    };

    match client
        .query(&statement, &[])
        .map(|row| {
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
        .compat()
        .await
    {
        Ok(stats) => Ok((stats, client)),
        Err(e) => Err((e, client)),
    }
}

async fn select_constraints(
    mut client: Client,
    table: &str,
) -> Result<(Vec<Constraint>, Client), (Error, Client)> {
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

    let statement = match client.prepare(&statement_str).compat().await {
        Ok(statement) => statement,
        Err(e) => return Err((e, client)),
    };

    match client
        .query(&statement, &[])
        .map(|row| {
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
        .compat()
        .await
    {
        Ok(rows) => Ok((rows, client)),
        Err(e) => Err((e, client)),
    }
}

// returns indexes (including primary keys) of a given table
async fn select_indexes(
    mut client: Client,
    table: &str,
) -> Result<(Vec<TableIndex>, Client), (Error, Client)> {
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

    let statement = match client.prepare(&statement_str).compat().await {
        Ok(statement) => statement,
        Err(e) => return Err((e, client)),
    };

    match client
        .query(&statement, &[])
        .map(|row| {
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
        .compat()
        .await
    {
        Ok(rows) => Ok((rows, client)),
        Err(e) => Err((e, client)),
    }
}

/// Takes the results of individual queries and generates the final table stats object
fn compile_table_stats(
    table: &str,
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
    }
}
