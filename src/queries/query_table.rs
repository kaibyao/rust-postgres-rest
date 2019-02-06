use crate::errors::ApiError;
// use postgres::types::ToSql;

use super::query_types::{convert_row_fields, Query, QueryResult};
use super::utils::validate_sql_name;
use crate::db::Connection;

pub fn query_table(conn: &Connection, query: &Query) -> Result<QueryResult, ApiError> {
    validate_sql_name(&query.table)?;
    let mut statement = String::from("SELECT");

    // building prepared statement
    for (i, column) in query.columns.iter().enumerate() {
        validate_sql_name(&column)?;

        if i == query.columns.len() - 1 {
            statement.push_str(&format!(" {}", &column));
        } else {
            statement.push_str(&format!(" {},", &column));
        }
    }

    // for i in 0..query.columns.len() {
    //     match i {
    //         0 => statement.push_str(&format!(" ${}", i + 1)),
    //         _ => statement.push_str(&format!(", ${}", i + 1)),
    //     }
    // }
    statement.push_str(&format!(" FROM {};", &query.table));

    // dbg!(&statement);

    // sending prepared statement to postgres
    let prep_statement = conn.prepare(&statement)?;

    // preparing statement params
    // let mut query_params: Vec<&ToSql> = vec![];
    // for column in query.columns.iter() {
    //     query_params.push(column);
    // }

    let results = prep_statement
        // .query(&query_params)?
        .query(&[])?
        .iter()
        .map(|row| convert_row_fields(&row))
        .collect();

    Ok(QueryResult::QueryTableResult(results))
}
