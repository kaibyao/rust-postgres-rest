use std::collections::HashMap;
use failure::Error;
use crate::db::Connection;

struct Column {
    column_name: String,
    column_type: String,
    is_nullable: bool,
    default_value: String,
}

/// Convenience type alias
pub type GetAllTableColumnsResult = HashMap<String, Vec<Column>>;

/// Retrieves all user-created table names and relevant column details
pub fn get_all_table_columns(conn: &Connection) -> Result<GetAllTableColumnsResult, Error> {
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
            let columns: Vec<Column> = vec![];
            table_columns.insert(table_name, columns);
        }

        // store column data for each table
        match table_columns.get(&table_name) {
            Some(columns) => columns.push(Column {
                column_name: row.get(1),
                column_type: row.get(4),
                is_nullable: row.get(2),
                default_value: row.get(3),
            }),
            None => {}
        }
    }

    // let rows: Vec<Table> = prep_statement.query(&[])?.iter().map(|row| row.get(0)).collect();
    Ok(table_columns)
}
