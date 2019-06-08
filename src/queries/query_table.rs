use super::foreign_keys::{convert_where_clause_str_to_fk_columns, ForeignKeyReference};
use super::postgres_types::{convert_row_fields, RowFields};
use super::query_types::{Query, QueryParams, QueryParamsSelect, QueryResult};
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
    let params;

    if let QueryParams::Select(query_params) = &query.params {
        params = query_params;
    } else {
        unreachable!("This function should never be called with params that aren’t shaped as a QueryParamsSelect.")
    }

    validate_sql_name(&params.table)?;

    // get list of every column being used in the query params (columns, where, distinct, group_by, order_by)
    let mut columns = params
        .columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    let mut where_columns = vec![];
    if let Some(where_clause_str) = &params.conditions {
        if let Some(where_fk_columns) = convert_where_clause_str_to_fk_columns(where_clause_str)? {
            where_columns = where_fk_columns;
        }
    }
    columns.extend(where_columns.iter().map(String::as_str));

    if let Some(s) = &params.distinct {
        columns.extend(s.split(','));
    }
    if let Some(s) = &params.group_by {
        columns.extend(s.split(','));
    }
    if let Some(s) = &params.order_by {
        columns.extend(s.split(','));
    }

    // need to parse distinct columns and columns for foreign key usage
    let fk_columns = ForeignKeyReference::from_query_columns(conn, &params.table, &columns)?;

    let (statement, prepared_values) = build_select_statement(params, fk_columns)?;
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

    let results: Result<Vec<RowFields>, ApiError> = prep_statement
        .query(&prep_values)?
        .iter()
        .map(|row| convert_row_fields(&row))
        .collect();

    Ok(QueryResult::QueryTableResult(results?))
}

fn build_select_statement(
    params: &QueryParamsSelect,
    fk_opts: Option<Vec<ForeignKeyReference>>,
) -> Result<(String, Vec<PreparedStatementValue>), ApiError> {
    let mut statement = vec!["SELECT"];

    let fks = if let Some(fks_inner) = fk_opts {
        fks_inner
    } else {
        vec![]
    };
    let is_fks_exist = !fks.is_empty();

    // DISTINCT clause if exists
    if let Some(distinct_str) = &params.distinct {
        statement.push(" DISTINCT ON (");

        let distinct_columns: Vec<&str> = distinct_str.split(',').collect();
        for (i, column_str_raw) in distinct_columns.iter().enumerate() {
            let trimmed = column_str_raw.trim();
            validate_where_column(trimmed)?;

            if let (true, Some(fk_ref)) = (
                is_fks_exist,
                ForeignKeyReference::find(&fks, &params.table, trimmed),
            ) {
                statement.push(fk_ref.table_referred.as_str());
                statement.push(".");
                statement.push(fk_ref.table_column_referred.as_str());
            } else {
                statement.push(trimmed);
            }

            if i < distinct_columns.len() - 1 {
                statement.push(", ");
            }
        }
        statement.push(")");
    }

    // dbg!(&params.columns);

    // building prepared statement
    for (i, column) in params.columns.iter().enumerate() {
        validate_where_column(&column)?;

        statement.push(" ");
        statement.push(&column);

        if i < params.columns.len() - 1 {
            statement.push(", ");
        }
    }

    statement.push(" FROM ");
    statement.push(&params.table);

    let mut prepared_values = vec![];
    if let Some(conditions) = &params.conditions {
        statement.push(" WHERE (");
        statement.push(conditions);
        statement.push(")");

        if let Some(prepared_values_opt) = &params.prepared_values {
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

    // GROUP BY statement
    if let Some(group_by_str) = &params.group_by {
        statement.push(" GROUP BY ");

        let group_by_columns: Vec<&str> = group_by_str.split(',').collect();
        for (i, column) in group_by_columns.iter().enumerate() {
            let trimmed = column.trim();
            validate_where_column(trimmed)?;
            statement.push(trimmed);

            if i < group_by_columns.len() - 1 {
                statement.push(", ");
            }
        }
    }

    // Append ORDER BY if the param exists
    let order_by_column_str = if let Some(order_by_column_str) = &params.order_by {
        lazy_static! {
            // case-insensitive search for ORDER BY direction
            static ref ORDER_BY_DIRECTION_RE: Regex = Regex::new(r"(?i) ASC| DESC").unwrap();
        }

        let order_by_columns: Result<Vec<&str>, ApiError> = order_by_column_str
            .split(',')
            .map(|column_str_raw| {
                let column = column_str_raw.trim();

                // using `is_match` first because it's faster than `find`
                if ORDER_BY_DIRECTION_RE.is_match(column) {
                    // we need to account for ASC and DESC directions
                    match ORDER_BY_DIRECTION_RE.find(column) {
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


                Ok(column)
            })
            .collect();

        order_by_columns?.join(", ")
    } else {
        "".to_string()
    };
    if order_by_column_str != "" {
        statement.push(" ORDER BY ");
        statement.push(&order_by_column_str);
    }

    // LIMIT
    let limit_str = params.limit.to_string();
    statement.push(" LIMIT ");
    statement.push(&limit_str);

    // OFFSET
    let offset_str = params.offset.to_string();
    if params.offset > 0 {
        statement.push(" OFFSET ");
        statement.push(&offset_str);
    }

    statement.push(";");

    Ok((statement.join(""), prepared_values))
}

#[cfg(test)]
mod build_select_statement_tests {
    use super::super::query_types::QueryParamsSelect;
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic_query() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn multiple_columns() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id, name FROM a_table LIMIT 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn distinct() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT DISTINCT ON (name, blah) id FROM a_table LIMIT 100;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn offset() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 1000 OFFSET 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn order_by() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table ORDER BY name, test LIMIT 1000;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn conditions() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table WHERE ((id > 10 OR id < 20) AND name = 'test') LIMIT 10;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn prepared_values() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
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
                panic!(e);
            }
        };
    }

    #[test]
    fn complex_query() {
        match build_select_statement(
            &QueryParamsSelect {
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
            None,
        ) {
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
                panic!(e);
            }
        };
    }
}
