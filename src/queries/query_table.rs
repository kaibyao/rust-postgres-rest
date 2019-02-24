use super::postgres_types::convert_row_fields;
use super::query_types::{Query, QueryResult};
use super::utils::validate_sql_name;
use crate::db::Connection;
use crate::errors::ApiError;

/// Returns the results of a `SELECT /*..*/ FROM {TABLE}` query
pub fn query_table(conn: &Connection, query: Query) -> Result<QueryResult, ApiError> {
    validate_sql_name(&query.params.table)?;
    let mut statement = String::from("SELECT");

    // DISTINCT clause if exists
    if let Some(distinct_str) = query.params.distinct {
        let distinct_columns: Vec<String> = distinct_str
            .split(',')
            .map(|column_str_raw| String::from(column_str_raw.trim()))
            .collect();

        for column in &distinct_columns {
            validate_sql_name(column)?;
        }

        statement.push_str(&format!(" DISTINCT ON ({}) ", distinct_columns.join(", ")));
    }

    // building prepared statement
    for (i, column) in query.params.columns.iter().enumerate() {
        validate_sql_name(&column)?;

        if i == query.params.columns.len() - 1 {
            statement.push_str(&format!(" {}", &column));
        } else {
            statement.push_str(&format!(" {},", &column));
        }
    }

    statement.push_str(&format!(" FROM {}", &query.params.table));

    // TODO: add WHERE parsing with prepared statements

    // TODO: add foreign key traversal

    // Append ORDER BY if the param exists
    if let Some(order_by_column_str) = query.params.order_by {
        let columns: Vec<String> = order_by_column_str
            .split(',')
            .map(|column_str_raw| String::from(column_str_raw.trim()))
            .collect();

        for column in &columns {
            validate_sql_name(column)?;
        }

        statement.push_str(&format!(" ORDER BY {}", columns.join(", ")));
    }

    // LIMIT
    statement.push_str(&format!(" LIMIT {}", query.params.limit));

    // OFFSET
    if query.params.offset > 0 {
        statement.push_str(&format!(" OFFSET {}", query.params.offset));
    }

    statement.push_str(";");
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
