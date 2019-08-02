use futures::{
    future::{err, Either, Future},
    stream::Stream,
};
use sqlparser::ast::Expr;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

use super::{
    foreign_keys::{fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::{convert_row_fields, ColumnTypeValue, RowFields},
    query_types::{QueryParamsDelete, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        get_columns_str, get_where_string, validate_table_name, validate_where_column,
        where_clause_str_to_ast,
    },
};
use crate::{db::connect, AppState, Error};

/// Returns the results of a `DELETE FROM {table} WHERE [conditions] [RETURNING [columns]]` query.
pub fn delete_table_rows(
    state: &AppState,
    params: QueryParamsDelete,
) -> impl Future<Item = QueryResult, Error = Error> {
    if let Err(e) = validate_table_name(&params.table) {
        return Either::A(err(e));
    }

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

    let mut column_expr_strings = fk_columns_from_where_ast(&where_ast);

    // RETURNING column foreign key references
    let mut is_return_rows = false;
    if let Some(v) = &params.returning_columns {
        for col in v.iter() {
            if let Err(e) = validate_where_column(col) {
                return Either::A(err(e));
            }
        }

        is_return_rows = true;
        column_expr_strings.extend(v.clone());
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

    // parse column_expr_strings for foreign key usage
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
            column_expr_strings,
        ))
        .and_then(move |(stats, fk_columns)| {
            let (statement_str, prepared_values) =
                match build_delete_statement(params, stats, fk_columns, where_ast) {
                    Ok((st, vals)) => (st, vals),
                    Err(e) => return Either::A(err(e)),
                };

            // TODO: abstract out this functionality because it's used everywhere
            let delete_rows_future =
                connect(&db_url_str)
                    .map_err(Error::from)
                    .and_then(move |mut conn| {
                        conn.prepare(&statement_str).map_err(Error::from).and_then(
                            move |statement| {
                                let prep_values: Vec<&dyn ToSql> =
                                    prepared_values.iter().map(|v| v as _).collect();

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

            Either::B(delete_rows_future)
        });

    Either::B(fk_future)
}

fn build_delete_statement(
    params: QueryParamsDelete,
    stats: Vec<TableColumnStat>,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: Expr,
) -> Result<(String, Vec<ColumnTypeValue>), Error> {
    let mut query_str_arr = vec!["DELETE FROM \n  ", &params.table];
    let mut prepared_statement_values = vec![];

    // build USING and WHERE foreign-key clauses
    let fk_using_clause;
    let mut fk_where_filter = String::from("");
    if !fks.is_empty() {
        query_str_arr.push("\nUSING\n  ");

        fk_using_clause = ForeignKeyReference::join_foreign_key_references(
            &fks,
            |(_referring_table, _referring_column, fk_table, _fk_column)| fk_table.to_string(),
            ",\n  ",
        );

        fk_where_filter = ForeignKeyReference::join_foreign_key_references(
            &fks,
            |(referring_table, referring_column, fk_table, fk_column)| {
                format!(
                    "{}.{} = {}.{}",
                    referring_table, referring_column, fk_table, fk_column
                )
            },
            " AND\n  ",
        );

        query_str_arr.push(&fk_using_clause);
    }

    // appending WHERE clauses to statement
    let (mut where_string, where_column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    if &where_string != "" || &fk_where_filter != "" {
        query_str_arr.push("\nWHERE (\n  ");

        if &where_string != "" {
            let (where_string_with_prepared_positions, prepared_values_vec) =
                ColumnTypeValue::generate_prepared_statement_from_ast_expr(
                    &where_ast,
                    &params.table,
                    &where_column_types,
                    None,
                )?;
            where_string = where_string_with_prepared_positions;
            prepared_statement_values.extend(prepared_values_vec);

            query_str_arr.push(&where_string);

            if fk_where_filter != "" {
                query_str_arr.push(" AND ");
            }
        }

        if &fk_where_filter != "" {
            query_str_arr.push(&fk_where_filter);
        }

        query_str_arr.push("\n)");
    }

    // returning_columns
    if let Some(returned_column_names) = &params.returning_columns {
        query_str_arr.push(" RETURNING ");

        let returning_columns_str = get_columns_str(returned_column_names, &params.table, &fks)?;
        query_str_arr.extend(returning_columns_str);
    }

    query_str_arr.push(";");

    Ok((query_str_arr.join(""), prepared_statement_values))
}

#[cfg(test)]
mod build_delete_statement_tests {
    use super::*;
    use crate::queries::{postgres_types::ColumnValue, query_types::QueryParamsDelete};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn simple() {
        let params = QueryParamsDelete {
            confirm_delete: Some("true".to_string()),
            conditions: None,
            returning_columns: None,
            table: "a_table".to_string(),
        };
        let stats = vec![TableColumnStat {
            column_name: "name".to_string(),
            column_type: "text",
            default_value: None,
            is_nullable: true,
            is_foreign_key: false,
            foreign_key_table: None,
            foreign_key_column: None,
            foreign_key_column_type: None,
            char_max_length: None,
            char_octet_length: None,
        }];
        let fks = vec![];

        let (sql_str, prepared_values) =
            build_delete_statement(params, stats, fks, Expr::Identifier("".to_string())).unwrap();

        assert_eq!(&sql_str, "DELETE FROM\n  a_table;");
        assert_eq!(prepared_values, vec![]);
    }

    #[test]
    fn fks_no_conditions() {
        let params = QueryParamsDelete {
            confirm_delete: Some("true".to_string()),
            conditions: None,
            returning_columns: None,
            table: "a_table".to_string(),
        };
        let stats = vec![
            TableColumnStat {
                column_name: "name".to_string(),
                column_type: "text",
                default_value: None,
                is_nullable: true,
                is_foreign_key: false,
                foreign_key_table: None,
                foreign_key_column: None,
                foreign_key_column_type: None,
                char_max_length: None,
                char_octet_length: None,
            },
            TableColumnStat {
                column_name: "b_id".to_string(),
                column_type: "int8",
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("b_table".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8"),
                char_max_length: None,
                char_octet_length: None,
            },
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["b_table.name".to_string()],
            referring_table: "a_table".to_string(),
            referring_column: "b_id".to_string(),
            referring_column_type: "int8",
            foreign_key_table: "b_table".to_string(),
            foreign_key_table_stats: vec![
                TableColumnStat {
                    column_name: "id".to_string(),
                    column_type: "int8",
                    default_value: None,
                    is_nullable: false,
                    is_foreign_key: false,
                    foreign_key_table: None,
                    foreign_key_column: None,
                    foreign_key_column_type: None,
                    char_max_length: None,
                    char_octet_length: None,
                },
                TableColumnStat {
                    column_name: "name".to_string(),
                    column_type: "text",
                    default_value: None,
                    is_nullable: true,
                    is_foreign_key: false,
                    foreign_key_table: None,
                    foreign_key_column: None,
                    foreign_key_column_type: None,
                    char_max_length: None,
                    char_octet_length: None,
                },
            ],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
            nested_fks: vec![],
        }];

        let (sql_str, prepared_values) =
            build_delete_statement(params, stats, fks, Expr::Identifier("".to_string())).unwrap();

        assert_eq!(&sql_str, "DELETE FROM\n  a_table;");
        assert_eq!(prepared_values, vec![]);
    }
}
