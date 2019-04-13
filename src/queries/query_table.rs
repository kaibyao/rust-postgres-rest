use super::postgres_types::convert_row_fields;
use super::query_types::{Query, QueryResult};
use super::utils::{validate_sql_name, validate_where_column};
use crate::db::Connection;
use crate::errors::ApiError;
use postgres::types::ToSql;
use regex::Regex;

#[derive(Debug, PartialEq)]
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
            validate_where_column(column)?;
        }

        statement.push_str(&format!(" DISTINCT ON ({})", distinct_columns.join(", ")));
    }
    // dbg!(&query.params.columns);
    // building prepared statement
    for (i, column) in query.params.columns.iter().enumerate() {
        validate_where_column(&column)?;

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

    // GROUP BY statement
    if let Some(group_by_str) = query.params.group_by {
        let group_bys: Vec<String> = group_by_str
            .split(',')
            .map(|group_by_col| String::from(group_by_col.trim()))
            .collect();

        for column in &group_bys {
            validate_where_column(column)?;
        }

        statement.push_str(&format!(" GROUP BY {}", group_bys.join(", ")));
    }

    // Append ORDER BY if the param exists
    if let Some(order_by_column_str) = query.params.order_by {
        let columns: Vec<String> = order_by_column_str
            .split(',')
            .map(|column_str_raw| String::from(column_str_raw.trim()))
            .collect();

        lazy_static! {
            static ref ORDER_DIRECTION_RE: Regex = Regex::new(r" ASC| DESC").unwrap();
        }

        for column in &columns {
            if ORDER_DIRECTION_RE.is_match(column) {
                // we need to account for ASC and DESC directions
                match ORDER_DIRECTION_RE.find(column) {
                    Some(order_direction_match) => {
                        let order_by_column = &column[..order_direction_match.start()];
                        validate_where_column(order_by_column)?;
                    }
                    None => {
                        validate_where_column(column)?;
                    }
                }
            } else {
                validate_where_column(column)?;
            }
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

#[cfg(test)]
mod build_select_statement_tests {
    use super::super::query_types::{QueryParams, QueryTasks};
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic_query() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 100;");
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn multiple_columns() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string(), "name".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id, name FROM a_table LIMIT 100;");
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn distinct() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: Some("name, blah".to_string()),
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT DISTINCT ON (name, blah) id FROM a_table LIMIT 100;"
                );
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn offset() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 1000,
                offset: 100,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 1000 OFFSET 100;");
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn order_by() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 1000,
                offset: 0,
                order_by: Some("name,test".to_string()),
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table ORDER BY name, test LIMIT 1000;"
                );
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn conditions() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: Some("(id > 10 OR id < 20) AND name = 'test'".to_string()),
                distinct: None,
                group_by: None,
                limit: 10,
                offset: 0,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table WHERE ((id > 10 OR id < 20) AND name = 'test') LIMIT 10;"
                );
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn prepared_values() {
        let query = Query {
            params: QueryParams {
                columns: vec!["id".to_string()],
                conditions: Some("(id > $1 OR id < $2) AND name = $3".to_string()),
                distinct: None,
                group_by: None,
                limit: 10,
                offset: 0,
                order_by: None,
                prepared_values: Some("10,20,'test'".to_string()),
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, prepared_values)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table WHERE ((id > $1 OR id < $2) AND name = $3) LIMIT 10;"
                );

                assert_eq!(
                    prepared_values,
                    vec![
                        PreparedStatementValue::Int4(10),
                        PreparedStatementValue::Int4(20),
                        PreparedStatementValue::String("test".to_string()),
                    ]
                );
            }
            Err(e) => {
                assert!(false, e);
            }
        };
    }

    #[test]
    fn complex_query() {
        let query = Query {
            params: QueryParams {
                columns: vec![
                    "id".to_string(),
                    "test_bigint".to_string(),
                    "test_bigserial".to_string(),
                ],
                conditions: Some("id = $1 AND test_name = $2".to_string()),
                distinct: Some("test_date,test_timestamptz".to_string()),
                group_by: None,
                limit: 10000,
                offset: 2000,
                order_by: Some("due_date DESC".to_string()),
                prepared_values: Some("46327143679919107,'a name'".to_string()),
                table: "a_table".to_string(),
            },
            req_body: None,
            task: QueryTasks::GetAllTables,
        };

        match build_select_statement(query) {
            Ok((sql, prepared_values)) => {
                assert_eq!(
                    &sql,
                    "SELECT DISTINCT ON (test_date, test_timestamptz) id, test_bigint, test_bigserial FROM a_table WHERE (id = $1 AND test_name = $2) ORDER BY due_date DESC LIMIT 10000 OFFSET 2000;"
                );

                assert_eq!(
                    prepared_values,
                    vec![
                        PreparedStatementValue::Int8(46_327_143_679_919_107i64),
                        PreparedStatementValue::String("a name".to_string()),
                    ]
                );
            }
            Err(e) => {
                assert!(false, format!("{}", e));
            }
        };
    }
}
