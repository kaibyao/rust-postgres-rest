use futures::future::{err, Either, Future};
use futures::stream::Stream;

use regex::Regex;
use sqlparser::sqlast::ASTNode;
use tokio_postgres::types::ToSql;

use super::foreign_keys::{
    fk_ast_nodes_from_where_ast, fk_columns_from_where_ast, where_clause_str_to_ast,
    ForeignKeyReference,
};
use super::postgres_types::{convert_row_fields, RowFields};
use super::query_types::QueryParamsSelect;
use super::utils::{validate_sql_name, validate_where_column};
use crate::db::Pool;
use crate::errors::ApiError;

#[derive(Debug, PartialEq)]
enum PreparedStatementValue {
    String(String),
    Int8(i64),
    Int4(i32),
}

/// Returns the results of a `SELECT /*..*/ FROM {TABLE}` query
pub fn select_table_rows(
    pool: &Pool,
    params: QueryParamsSelect,
) -> impl Future<Item = Vec<RowFields>, Error = ApiError> {
    if let Err(e) = validate_sql_name(&params.table) {
        return Either::A(err::<Vec<RowFields>, ApiError>(e));
    }

    // get list of every column being used in the query params (columns, where, distinct, group_by, order_by). Used for finding all foreign key references
    let mut columns = params.columns.clone();

    // WHERE clause foreign key references
    let where_ast = match &params.conditions {
        Some(where_clause_str) => match where_clause_str_to_ast(where_clause_str) {
            Ok(ast_opt) => match ast_opt {
                Some(ast) => ast,
                None => ASTNode::SQLIdentifier("".to_string()),
            },
            Err(_) => {
                return Either::A(err::<Vec<RowFields>, ApiError>(ApiError::generate_error(
                    "INVALID_SQL_SYNTAX",
                    ["WHERE", where_clause_str].join(":"),
                )));
            }
        },
        None => ASTNode::SQLIdentifier("".to_string()),
    };
    let where_fk_columns = fk_columns_from_where_ast(&where_ast);

    columns.extend(where_fk_columns);
    if let Some(v) = &params.distinct {
        columns.extend(v.clone());
    }
    if let Some(v) = &params.group_by {
        columns.extend(v.clone());
    }
    if let Some(v) = &params.order_by {
        columns.extend(v.clone());
    }

    let pool = pool.clone();

    // parse columns for foreign key usage
    let fk_future =
        ForeignKeyReference::from_query_columns(pool.clone(), params.table.clone(), columns)
            .map_err(ApiError::from)
            .and_then(move |fk_columns| {
                dbg!(&fk_columns);

                let (statement_str, prepared_values) =
                    match build_select_statement(&params, fk_columns, where_ast) {
                        Ok((stmt, prep_vals)) => (stmt, prep_vals),
                        Err(e) => return Either::A(err::<Vec<RowFields>, ApiError>(e)),
                    };

                dbg!(&statement_str);
                dbg!(&prepared_values);

                // sending prepared statement to postgres
                let select_rows_future =
                    pool.connection()
                        .map_err(ApiError::from)
                        .and_then(move |mut conn| {
                            conn.client
                                .prepare(&statement_str)
                                .map_err(ApiError::from)
                                .and_then(move |statement| {
                                    let prep_values: Vec<&dyn ToSql> = if prepared_values.is_empty()
                                    {
                                        vec![]
                                    } else {
                                        prepared_values
                                            .iter()
                                            .map(|val| {
                                                let val_to_sql: &dyn ToSql = match val {
                                                    PreparedStatementValue::Int4(val_i32) => {
                                                        val_i32
                                                    }
                                                    PreparedStatementValue::Int8(val_i64) => {
                                                        val_i64
                                                    }
                                                    PreparedStatementValue::String(val_string) => {
                                                        val_string
                                                    }
                                                };
                                                val_to_sql
                                            })
                                            .collect()
                                    };

                                    dbg!(&prep_values);

                                    conn.client
                                        .query(&statement, &prep_values)
                                        .then(|result| match result {
                                            Ok(row) => match convert_row_fields(&row) {
                                                Ok(row_fields) => Ok(row_fields),
                                                Err(e) => Err(e),
                                            },
                                            Err(e) => Err(ApiError::from(e)),
                                        })
                                        .collect()
                                })
                        });

                Either::B(select_rows_future)
            });

    Either::B(fk_future)
}

fn build_select_statement(
    params: &QueryParamsSelect,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: ASTNode,
) -> Result<(String, Vec<PreparedStatementValue>), ApiError> {
    let mut statement = vec!["SELECT "];
    let is_fks_exist = !fks.is_empty();

    // DISTINCT clause if exists
    if let Some(distinct_columns) = &params.distinct {
        statement.push("DISTINCT ON (");
        statement.extend(get_column_str(distinct_columns, &params.table, &fks)?);
        statement.push(") ");
    }

    // dbg!(&params.columns);

    // building column selection
    statement.extend(get_column_str(&params.columns, &params.table, &fks)?);

    statement.push(" FROM ");
    statement.push(&params.table);

    // build inner join expression
    let inner_join_str = if is_fks_exist {
        ForeignKeyReference::inner_join_expr(&fks)
    } else {
        "".to_string()
    };
    if is_fks_exist {
        statement.push(" INNER JOIN ");
        statement.push(&inner_join_str);
    }

    // building WHERE string
    let where_str = params.conditions.as_ref().map_or("", |s| s.as_str());
    let where_string = get_where_string(where_str, &mut where_ast, &params.table, &fks);

    let mut prepared_values = vec![];
    if &where_string != "" {
        statement.push(" WHERE (");
        statement.push(&where_string);
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
    if let Some(group_by_columns) = &params.group_by {
        statement.push(" GROUP BY ");
        statement.extend(get_column_str(group_by_columns, &params.table, &fks)?);
    }

    // Append ORDER BY if the param exists
    if let Some(order_by_columns) = &params.order_by {
        statement.push(" ORDER BY ");

        lazy_static! {
            // case-insensitive search for ORDER BY direction
            static ref ORDER_BY_DIRECTION_RE: Regex = Regex::new(r"(?i) asc| desc").unwrap();
        }

        for (i, column) in order_by_columns.iter().enumerate() {
            // using `is_match` first because it's faster than `find`
            let (sql_column, order_by_direction): (&str, &str) =
                if ORDER_BY_DIRECTION_RE.is_match(column) {
                    // separate the column string from the direction string
                    match ORDER_BY_DIRECTION_RE.find(column) {
                        Some(order_direction_match) => {
                            let order_by_column = &column[..order_direction_match.start()];
                            validate_where_column(order_by_column)?;

                            let order_by_direction = &column[order_direction_match.start()..];
                            (order_by_column, order_by_direction)
                        }
                        None => {
                            validate_where_column(column)?;
                            (column, " asc")
                        }
                    }
                } else {
                    validate_where_column(column)?;
                    (column, " asc")
                };

            if let (true, Some((fk_ref, fk_column))) = (
                is_fks_exist,
                ForeignKeyReference::find(&fks, &params.table, sql_column),
            ) {
                statement.push(fk_ref.table_referred.as_str());
                statement.push(".");
                statement.push(fk_column);
            } else {
                statement.push(sql_column);
            }

            statement.push(if order_by_direction == " desc" {
                " DESC"
            } else {
                " ASC"
            });

            if i < order_by_columns.len() - 1 {
                statement.push(", ");
            }
        }
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

/// Generates a string of column names delimited by commas. Foreign keys are correctly accounted for.
fn get_column_str<'a>(
    columns: &'a [String],
    table: &'a str,
    fks: &'a [ForeignKeyReference],
) -> Result<Vec<&'a str>, ApiError> {
    let mut statement: Vec<&str> = vec![];
    let is_fks_exist = !fks.is_empty();

    // no FKs exist, just add columns with commas in between
    if !is_fks_exist {
        for (i, column) in columns.iter().enumerate() {
            statement.push(column);

            if i < columns.len() - 1 {
                statement.push(", ");
            }
        }

        return Ok(statement);
    }

    // correctly account for FK column references
    for (i, column) in columns.iter().enumerate() {
        validate_where_column(column)?;

        if let (true, Some((fk_ref, fk_column))) = (
            !fks.is_empty(),
            ForeignKeyReference::find(fks, table, column),
        ) {
            statement.push(fk_ref.table_referred.as_str());
            statement.push(".");
            statement.push(fk_column);

            // AS syntax (to avoid ambiguous columns)
            statement.push(" AS \"");
            statement.push(column);
            statement.push("\"");
        } else {
            statement.push(table);
            statement.push(".");
            statement.push(column);

            // AS syntax (to avoid ambiguous columns)
            statement.push(" AS \"");
            statement.push(column);
            statement.push("\"");
        }

        if i < columns.len() - 1 {
            statement.push(", ");
        }
    }

    Ok(statement)
}

/// Generates the WHERE clause after taking foreign keys into account.
fn get_where_string<'a>(
    where_str: &str,
    where_ast: &mut ASTNode,
    table: &str,
    fks: &'a [ForeignKeyReference],
) -> String {
    let where_fk_ast_nodes = fk_ast_nodes_from_where_ast(where_ast);

    if where_fk_ast_nodes.is_empty() {
        return where_str.to_string();
    }

    for (incorrect_column_name, ast_node) in where_fk_ast_nodes {
        if let (true, Some((fk_ref, fk_column))) = (
            !fks.is_empty(),
            ForeignKeyReference::find(fks, table, &incorrect_column_name),
        ) {
            let replacement_node = match ast_node {
                ASTNode::SQLQualifiedWildcard(_wildcard_vec) => {
                    ASTNode::SQLQualifiedWildcard(vec![
                        fk_ref.table_referred.clone(),
                        fk_column.to_string(),
                    ])
                }
                ASTNode::SQLCompoundIdentifier(_nested_fk_column_vec) => {
                    ASTNode::SQLCompoundIdentifier(vec![
                        fk_ref.table_referred.clone(),
                        fk_column.to_string(),
                    ])
                }
                _ => unimplemented!(
                    "The WHERE clause HashMap only contains wildcards and compound identifiers."
                ),
            };

            *ast_node = replacement_node;
        }
    }

    where_ast.to_string()
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
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
                distinct: Some(vec!["name".to_string(), "blah".to_string()]),
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                prepared_values: None,
                table: "a_table".to_string(),
            },
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
                order_by: Some(vec!["name".to_string(), "test".to_string()]),
                prepared_values: None,
                table: "a_table".to_string(),
            },
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table ORDER BY name ASC, test ASC LIMIT 1000;"
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
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
                distinct: Some(vec![
                    "test_date".to_string(),
                    "test_timestamptz".to_string(),
                ]),
                group_by: None,
                limit: 10000,
                offset: 2000,
                order_by: Some(vec!["due_date desc".to_string()]),
                prepared_values: Some("46327143679919107,'a name'".to_string()),
                table: "a_table".to_string(),
            },
            vec![],
            ASTNode::SQLIdentifier("".to_string()),
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
