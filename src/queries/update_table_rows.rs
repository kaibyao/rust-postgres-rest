use super::{
    foreign_keys::{fk_ast_nodes_from_where_ast, fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::{convert_row_fields, ColumnTypeValue, RowFields},
    query_types::{QueryParamsUpdate, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement},
    utils::{
        generate_returning_clause, validate_alias_identifier, validate_table_name,
        validate_where_column, where_clause_str_to_ast, PreparedStatementValue, UpsertResult,
    },
};
use crate::{db::connect, AppState, Error};
use futures::{
    future::{err, loop_fn, Either, Future, Loop},
    stream::Stream,
};
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

    // get list of every column being used in the query params (from, where, returning_columns).
    // Used for finding all foreign key references.
    let mut columns: Vec<String> = match &params.from {
        Some(from_cols) => from_cols
            .iter()
            .map(|col| {
                if let Ok(Some((actual_column_ref, _alias))) = validate_alias_identifier(col) {
                    actual_column_ref.to_string()
                } else {
                    col.to_string()
                }
            })
            .collect(),
        None => vec![],
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

    // parse columns for foreign key usage
    let db_url_str = state.config.db_url.to_string();
    let addr_clone = if let Some(addr) = &state.stats_cache_addr {
        Some(addr.clone())
    } else {
        None
    };
    let fk_future = ForeignKeyReference::from_query_columns(
        state.config.db_url,
        Arc::new(addr_clone),
        params.table.clone(),
        columns,
    )
    .and_then(move |fk_columns| {
        let (statement_str, prepared_values) =
            match build_update_statement(params, fk_columns, where_ast) {
                Ok((stmt, prep_vals)) => (stmt, prep_vals),
                Err(e) => return Either::A(err(e)),
            };

        let update_rows_future =
            connect(&db_url_str)
                .map_err(Error::from)
                .and_then(move |mut conn| {
                    conn.prepare(&statement_str)
                        .map_err(Error::from)
                        .and_then(move |statement| {
                            let prep_values: Vec<&dyn ToSql> = if prepared_values.is_empty() {
                                vec![]
                            } else {
                                prepared_values.iter().map(|val| val.to_sql()).collect()
                            };

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
                        })
                });

        Either::B(update_rows_future)
    });

    Either::B(fk_future)
}

/// Returns the UPDATE query statement string and a vector of prepared values.
fn build_update_statement(
    params: QueryParamsUpdate,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: Expr,
) -> Result<(String, Vec<PreparedStatementValue>), Error> {
    let mut query_str_arr = ["UPDATE ", &params.table, " SET "];
    let mut prepared_statement_values = vec![];

    // for entry in params.column_values

    Ok((query_str_arr.join(""), prepared_statement_values))
}

// /// Generates the `SET <columns>=<values>` string and the prepared values of the UPDATE
// statement. fn generate_update_params(
//     column_values: Map<String, Value>,
// ) -> Result<(String, Vec<ColumnTypeValue>), Error> {

// }
