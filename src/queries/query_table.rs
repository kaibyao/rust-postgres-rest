use failure::Error;

use super::query_types::{convert_row_fields, Query, QueryResult};
use super::utils::validate_sql_name;
use crate::db::Connection;

pub fn query_table(conn: &Connection, query: &Query) -> Result<QueryResult, Error> {
    validate_sql_name(&query.table)?;

    let mut statement = String::from("SELECT");
    for i in 0..query.columns.len() {
        match i {
            0 => statement.push_str(&format!(" ${}", i + 1)),
            _ => statement.push_str(&format!(", ${}", i + 1)),
        }
    }
    statement.push_str(&format!(" FROM {};", &query.table));
    dbg!(&statement);
    let prep_statement = conn.prepare(&statement)?;

    // let results = prep_statement.query(&[&query.columns.join(", "), &query.table]);
    // let query_params: Vec<&String> = query
    //     .columns // Vec<String>
    //     .iter()
    //     .map(|c| &c) // Vec<&str>
    //     .collect/*::<Vec<&String>>*/();

    // let test: () = &[&query.columns.join(", "), &query.table];
    // let test2: () = query_params[..];
    let results = prep_statement
        // .query(&[&query.columns.join(", "), &query.table])?
        .query(&query.columns[..])?
        .iter()
        .map(|row| {
            dbg!(&row);
            convert_row_fields(&row)
        })
        .collect();

    dbg!(&results);

    Ok(QueryResult::QueryTableResult(results))
}
