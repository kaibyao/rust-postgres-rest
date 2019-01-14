use failure::Error;
use r2d2_postgres::postgres::types::FromSql;
use std::collections::HashMap;
// use std::io::{Error as StdError, ErrorKind};

use super::query_types::Query;
use super::utils::validate_sql_name;
use crate::db::Connection;

pub fn query_table(
    conn: &Connection,
    query: &Query,
) -> Result<Vec<HashMap<String, Box<FromSql>>>, Error> {
    validate_sql_name(&query.table)?;

    let statement = "
        SELECT
            $1
        FROM
            $2
        ;";
    let prep_statement = conn.prepare(statement)?;

    let num_columns = &query.columns.len();
    let mut rows = vec![];
    for row in prep_statement
        .query(&[&query.columns.join(", "), &query.table])?
        .iter()
    {
        let mut converted_row = HashMap::new();
        for i in 0..*num_columns {
            converted_row.insert(query.columns[i].clone(), Box::new(row.get(i)));
        }
        rows.push(converted_row);
    }

    Ok(rows)
}
