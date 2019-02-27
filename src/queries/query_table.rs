use super::postgres_types::convert_row_fields;
use super::query_types::{Query, QueryResult};
use super::utils::validate_sql_name;
use crate::db::Connection;
use crate::errors::ApiError;
use postgres::types::ToSql;
use regex::Regex;

#[derive(Debug)]
enum PreparedStatementValue {
    String(String),
    Int8(i64),
    Int4(i32),
}

/// Returns the results of a `SELECT /*..*/ FROM {TABLE}` query
pub fn query_table(conn: &Connection, query: Query) -> Result<QueryResult, ApiError> {
    validate_sql_name(&query.params.table)?;
    let (statement, prepared_values) = build_select_statement(query)?;
    // dbg!(&statement);
    // dbg!(&prepared_values);

    // sending prepared statement to postgres
    let prep_statement = conn.prepare(&statement)?;
    let prep_values: Vec<&ToSql> = if prepared_values.is_empty() {
        vec![]
    } else {
        prepared_values
            .iter()
            .map(|val| {
                let val_to_sql: &ToSql = match val {
                    PreparedStatementValue::Int4(val_i32) => val_i32,
                    PreparedStatementValue::Int8(val_i64) => val_i64,
                    PreparedStatementValue::String(val_string) => val_string,
                };
                val_to_sql
            })
            .collect()
    };

    // dbg!(&prep_values);

    let results = prep_statement
        .query(&prep_values)?
        .iter()
        .map(|row| convert_row_fields(&row))
        .collect();

    Ok(QueryResult::QueryTableResult(results))
}

fn build_select_statement(query: Query) -> Result<(String, Vec<PreparedStatementValue>), ApiError> {
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

    let mut prepared_values = vec![];
    if let Some(conditions) = query.params.conditions {
        statement.push_str(&format!(" WHERE ({})", conditions));

        if let Some(prepared_values_opt) = query.params.prepared_values {
            lazy_static! {
                // need to parse integer strings as i32 or i64 so we don’t run into conversion errors
                // (because rust-postgres attempts to convert really large integer strings as i32, which fails)
                static ref INTEGER_RE: Regex = Regex::new(r"^\d+$").unwrap();

                // anything in quotes should be forced as a string
                static ref STRING_RE: Regex = Regex::new(r#"^['"](.+)['"]$"#).unwrap();
            }

            let prepared_values_vec = prepared_values_opt
                .split(',')
                .map(|val| {
                    let val_str = val.trim();

                    if STRING_RE.is_match(val_str) {
                        let captures = STRING_RE.captures(val_str).unwrap();
                        let val_string = captures.get(1).unwrap().as_str().to_string();

                        return PreparedStatementValue::String(val_string);
                    } else if INTEGER_RE.is_match(val_str) {
                        if let Ok(val_i32) = val_str.parse::<i32>() {
                            return PreparedStatementValue::Int4(val_i32);
                        } else if let Ok(val_i64) = val_str.parse::<i64>() {
                            return PreparedStatementValue::Int8(val_i64);
                        }
                    }

                    PreparedStatementValue::String(val_str.to_string())
                })
                .collect();
            prepared_values = prepared_values_vec;
        }
    }

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

    Ok((statement, prepared_values))
}
