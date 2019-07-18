use super::{
    foreign_keys::{fk_ast_nodes_from_where_ast, fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::{
        convert_row_fields, ColumnTypeValue, PreparedStatementValue, RowFields, UpsertResult,
    },
    query_types::{QueryParamsUpdate, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        generate_returning_clause, get_db_column_str, get_where_string, validate_alias_identifier,
        validate_table_name, validate_where_column, where_clause_str_to_ast,
    },
};
use crate::{db::connect, AppState, Error};
use futures::{
    future::{err, Either, Future},
    stream::Stream,
};
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::{Map, Value as JsonValue};
use sqlparser::ast::{Expr, Value as SqlValue};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{types::ToSql, Client};

/// Runs an UPDATE query on the selected table rows.
pub fn update_table_rows(
    state: &AppState,
    params: QueryParamsUpdate,
) -> impl Future<Item = QueryResult, Error = Error> {
    if let Err(e) = validate_table_name(&params.table) {
        return Either::A(err(e));
    }

    // Get list of every column in the query (column_values, conditions, returning_columns). Used
    // for finding all foreign key references
    let mut columns: Vec<String> = match params
        .column_values
        .iter()
        .map(|(col, _val)| -> Result<String, Error> {
            match validate_alias_identifier(col)? {
                Some((actual_column_ref, _alias)) => Ok(actual_column_ref.to_string()),
                _ => Ok(col.clone()),
            }
        })
        .collect::<Result<Vec<String>, Error>>()
    {
        Ok(columns) => columns,
        Err(e) => return Either::A(err(e)),
    };

    // WHERE clause foreign key references
    let where_ast = match &params.conditions {
        Some(where_clause_str) => match where_clause_str_to_ast(where_clause_str) {
            Ok(ast_opt) => match ast_opt {
                Some(ast) => ast,
                None => Expr::Identifier("".to_string()),
            },
            Err(_e) => {
                return Either::A(err(Error::generate_error(
                    "INVALID_SQL_SYNTAX",
                    ["WHERE", where_clause_str].join(":"),
                )));
            }
        },
        None => Expr::Identifier("".to_string()),
    };
    columns.extend(fk_columns_from_where_ast(&where_ast));

    let mut is_return_rows = false;
    if let Some(v) = &params.returning_columns {
        is_return_rows = true;
        columns.extend(v.clone());
    }

    let db_url_str = state.config.db_url.to_string();

    // get table stats for building query (we need to know the column types)
    let table_clone = params.table.clone();
    let stats_future = connect(&db_url_str)
        .map_err(Error::from)
        .and_then(move |mut conn| {
            select_column_stats_statement(&mut conn, &table_clone)
                .map_err(Error::from)
                .and_then(move |statement| {
                    let q = conn.query(&statement, &[]);
                    select_column_stats(q).map_err(Error::from)
                })
        });

    // parse columns for foreign key usage
    let addr_clone = if let Some(addr) = &state.stats_cache_addr {
        Some(addr.clone())
    } else {
        None
    };

    let fk_future = stats_future
        .join(ForeignKeyReference::from_query_columns(
            state.config.db_url,
            Arc::new(addr_clone),
            params.table.clone(),
            columns,
        ))
        .and_then(move |(stats, fk_columns)| {
            let column_types: HashMap<String, String> =
                TableColumnStat::stats_to_column_types(stats.clone());

            let (statement_str, prepared_values) =
                match build_update_statement(params, column_types, stats, fk_columns, where_ast) {
                    Ok((stmt, prep_vals)) => (stmt, prep_vals),
                    Err(e) => return Either::A(err(e)),
                };

            let update_rows_future =
                connect(&db_url_str)
                    .map_err(Error::from)
                    .and_then(move |mut conn| {
                        conn.prepare(&statement_str).map_err(Error::from).and_then(
                            move |statement| {
                                let mut prep_values: Vec<&dyn ToSql> = vec![];
                                for val in prepared_values.iter() {
                                    prep_values.push(val);
                                }

                                if is_return_rows {
                                    let return_rows_future = conn
                                        .query(&statement, &prep_values)
                                        .collect()
                                        .map_err(Error::from)
                                        .and_then(|rows| {
                                            match rows
                                                .iter()
                                                .map(|row| convert_row_fields(&row))
                                                .collect::<Result<Vec<RowFields>, Error>>()
                                            {
                                                Ok(row_fields) => {
                                                    Ok(QueryResult::QueryTableResult(row_fields))
                                                }
                                                Err(e) => Err(e),
                                            }
                                        });

                                    Either::A(return_rows_future)
                                } else {
                                    let return_row_count_future = conn
                                        .execute(&statement, &prep_values)
                                        .then(move |result| match result {
                                            Ok(num_rows) => {
                                                Ok(QueryResult::from_num_rows_affected(num_rows))
                                            }
                                            Err(e) => Err(Error::from(e)),
                                        });

                                    Either::B(return_row_count_future)
                                }
                            },
                        )
                    });

            Either::B(update_rows_future)
        });

    Either::B(fk_future)
}

/// Returns the UPDATE query statement string and a vector of prepared values.
fn build_update_statement(
    params: QueryParamsUpdate,
    column_types: HashMap<String, String>,
    stats: Vec<TableColumnStat>,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: Expr,
) -> Result<(String, Vec<ColumnTypeValue>), Error> {
    // I would prefer this to be &str instead of String, but I haven't figured out yet the best way
    // to append prepared_value_pos usize as a &str.
    let mut query_str_arr = vec![
        "UPDATE ".to_string(),
        params.table.clone(),
        " SET ".to_string(),
    ];
    let mut prepared_statement_values = vec![];
    let mut prepared_value_pos: usize = 1;
    let mut prepared_value_pos_vec = vec![];
    // let mut from_tables = vec![];

    // Convert JSON object and append query_str and prepared_statement_values with column values
    let column_values_len = params.column_values.len();
    for (i, (col, val)) in params.column_values.iter().enumerate() {
        let column_type = column_types
            .get(col)
            .ok_or_else(|| Error::generate_error("TABLE_COLUMN_TYPE_NOT_FOUND", col.clone()))?;

        // pretty sure function in a loop is a zero-cost abstraction?
        let mut append_prepared_value = |val: &JsonValue| -> Result<(), Error> {
            let val = ColumnTypeValue::from_json(column_type, &val)?;
            prepared_statement_values.push(val);

            let actual_column_tokens = get_db_column_str(col, &params.table, &fks, false)?;

            query_str_arr.push(actual_column_tokens.join(""));

            let prepared_value_pos_str = prepared_value_pos.to_string();
            query_str_arr.push([" = $", &prepared_value_pos_str].join(""));
            prepared_value_pos_vec.push(prepared_value_pos_str);
            prepared_value_pos += 1;

            Ok(())
        };

        // check for expression used as a column value
        if let Some(val_str) = val.as_str() {
            lazy_static! {
                // check for strings
                static ref STRING_RE: Regex = Regex::new(r#"^['"](.+)['"]$"#).unwrap();
            }

            if STRING_RE.is_match(val_str) {
                // column value is a string
                let captures = STRING_RE.captures(val_str).unwrap();
                let val_string = captures.get(1).unwrap().as_str().to_string();
                let val = JsonValue::String(val_string);

                append_prepared_value(&val)?;
            } else {
                // column value is an expression, parse foreign key usage
                let actual_column_tokens = get_db_column_str(col, &params.table, &fks, false)?;
                let actual_value_tokens = get_db_column_str(val_str, &params.table, &fks, false)?;

                let mut assignment_str: Vec<&str> = vec![];
                assignment_str.extend(actual_column_tokens);
                assignment_str.push(" = ");
                assignment_str.extend(actual_value_tokens);
                query_str_arr.push(assignment_str.join(""));
            }
        } else {
            append_prepared_value(&val)?;
        }

        if i < column_values_len - 1 {
            query_str_arr.push(", ".to_string());
        }
    }

    // building WHERE string
    let where_str = params.conditions.as_ref().map_or("", |s| s.as_str());
    let (mut where_string, where_column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    if &where_string != "" {
        query_str_arr.push(" WHERE (".to_string());

        // parse through the `WHERE` AST and return a tuple: (expression-with-prepared-params
        // string, Vec of tuples (position, Value)).
        let (where_string_with_prepared_positions, prepared_values_vec) =
            PreparedStatementValue::generate_prepared_statement_from_ast_expr(
                &where_ast,
                Some(&mut prepared_value_pos),
            )?;
        where_string = where_string_with_prepared_positions;

        // prepared_statement_values.extend(prepared_values_vec.into_iter().map(|prep_value|
        // ColumnTypeValue::from_prepared_statement_value(column_type: &str, value:
        // PreparedStatementValue))); prepared_values = prepared_values_vec;

        query_str_arr.push(where_string);
        query_str_arr.push(")".to_string());
    }

    // "from" should just take into account FK dot-syntax

    // returning_columns

    // table

    Ok((query_str_arr.join(""), prepared_statement_values))
}
