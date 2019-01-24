use crate::db::Connection;
use failure::Error;
use std::collections::HashMap;

use super::query_types::{GetAllTableColumnsColumn, QueryResult};

/// Retrieves all user-created table names and relevant column details
pub fn get_all_table_columns(conn: &Connection) -> Result<QueryResult, Error> {
    let statement = "
        SELECT
            table_name,
            column_name,
            is_nullable,
            column_default,
            udt_name
        FROM
            information_schema.columns
        WHERE table_schema='public'
        ORDER BY table_name, column_name;";
    let prep_statement = conn.prepare(statement)?;

    let mut table_columns = HashMap::new();

    for row in prep_statement.query(&[])?.iter() {
        let table_name: String = row.get(0);

        // create hashmap key if a column for a table has not yet been stored
        if !table_columns.contains_key(&table_name) {
            let columns: Vec<GetAllTableColumnsColumn> = vec![];
            table_columns.insert(table_name.clone(), columns);
        }

        // store column data for each table
        if let Some(columns) = table_columns.get_mut(&table_name) {
            let is_nullable: Option<String> = row.get(2);

            // for column in row.columns() {
            //     println!("name(): {:?}", column.name());
            //     println!("type_(): {:?}", column.type_());
            //     println!("type.schema(): {:?}", column.type_().schema());
            //     println!("type.name(): {:?}", column.type_().name());
            //     /*
            //     name(): "udt_name"
            //     type_(): Type(Varchar)
            //     type.schema(): "pg_catalog"
            //     type.name(): "varchar"
            //     */
            // }
            // println!("{:?}", row.columns());
            // [Column { name: "table_name", type_: Type(Varchar) }, Column { name: "column_name", type_: Type(Varchar) }, Column { name: "is_nullable", type_: Type(Varchar) }, Column { name: "column_default", type_: Type(Varchar) }, Column { name: "udt_name", type_: Type(Varchar) }]

            columns.push(GetAllTableColumnsColumn {
                column_name: row.get(1),
                column_type: row.get(4),
                is_nullable: match is_nullable {
                    Some(is_nullable_string) => {
                        if is_nullable_string.eq("true") {
                            Some(true)
                        } else {
                            Some(false)
                        }
                    }
                    None => None,
                },
                default_value: row.get(3),
            });
        }
    }

    Ok(QueryResult::GetAllTableColumnsResult(table_columns))
}
