use futures::future::{err, Either, Future};
use sqlparser::ast::Expr;
use std::sync::Arc;

use super::{
    foreign_keys::{fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::ColumnTypeValue,
    query_types::{QueryParamsDelete, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        conditions_params_to_ast, generate_query_result_from_db, get_columns_str, get_where_string,
        validate_alias_identifier, validate_table_name,
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

    // WHERE clause w/ foreign key references
    let where_ast = match conditions_params_to_ast(&params.conditions) {
        Ok(ast) => ast,
        Err(e) => return Either::A(err(e)),
    };

    let mut column_expr_strings = fk_columns_from_where_ast(&where_ast);

    // RETURNING column foreign key references
    let mut is_return_rows = false;
    let returning_column_strs;
    if let Some(columns) = &params.returning_columns {
        let returning_column_strs_result = columns
            .iter()
            .map(|col| {
                if let Some((actual_column_ref, _alias)) = validate_alias_identifier(col)? {
                    Ok(actual_column_ref.to_string())
                } else {
                    Ok(col.to_string())
                }
            })
            .collect::<Result<Vec<String>, Error>>();

        match returning_column_strs_result {
            Ok(column_strs) => returning_column_strs = column_strs,
            Err(e) => return Either::A(err(e)),
        };

        is_return_rows = true;
        column_expr_strings.extend(returning_column_strs);
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

            let delete_rows_future = generate_query_result_from_db(
                &db_url_str,
                statement_str,
                prepared_values,
                is_return_rows,
            );

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
    let mut query_str_arr = vec!["DELETE FROM\n  ", &params.table];
    let mut prepared_statement_values = vec![];

    // appending WHERE clauses to statement
    let fk_using_clause;
    let mut fk_where_filter = String::from("");

    // build USING and WHERE foreign-key clauses
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

    let (mut where_string, where_column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    if &where_string != "" {
        query_str_arr.push("\nWHERE (\n  ");

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

        if &fk_where_filter != "" {
            query_str_arr.push(" AND\n  ");
            query_str_arr.push(&fk_where_filter);
        }

        query_str_arr.push("\n)");
    }

    // returning_columns
    if let Some(returned_column_names) = &params.returning_columns {
        query_str_arr.push("\nRETURNING\n  ");

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
        let fks = vec![];

        let (sql_str, prepared_values) =
            build_delete_statement(params, stats, fks, Expr::Identifier("".to_string())).unwrap();

        assert_eq!(&sql_str, "DELETE FROM\n  a_table;");
        assert_eq!(prepared_values, vec![]);
    }

    #[test]
    fn return_fk_columns() {
        let params = QueryParamsDelete {
            confirm_delete: Some("true".to_string()),
            conditions: None,
            returning_columns: Some(vec!["b_id.id".to_string()]),
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
            original_refs: vec!["b_id.id".to_string()],
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

        assert_eq!(
            &sql_str,
            "DELETE FROM\n  a_table\nUSING\n  b_table\nRETURNING\n  b_table.id AS \"b_id.id\";"
        );
        assert_eq!(prepared_values, vec![]);
    }

    #[test]
    fn fks_conditions() {
        let conditions = "id = 1";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();
        let params = QueryParamsDelete {
            confirm_delete: Some("true".to_string()),
            conditions: Some(conditions.to_string()),
            returning_columns: None,
            table: "a_table".to_string(),
        };
        let stats = vec![
            TableColumnStat {
                column_name: "id".to_string(),
                column_type: "int8",
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
            original_refs: vec!["b_id.id".to_string()],
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
            build_delete_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(
            &sql_str,
            "DELETE FROM\n  a_table\nUSING\n  b_table\nWHERE (\n  a_table.id = $1 AND\n  a_table.b_id = b_table.id\n);"
        );
        assert_eq!(
            prepared_values,
            vec![ColumnTypeValue::BigInt(ColumnValue::NotNullable(1))]
        );
    }

    #[test]
    fn fks_conditions_with_fk() {
        let conditions = "b_id.id = 1";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();
        let params = QueryParamsDelete {
            confirm_delete: Some("true".to_string()),
            conditions: Some(conditions.to_string()),
            returning_columns: None,
            table: "a_table".to_string(),
        };
        let stats = vec![
            TableColumnStat {
                column_name: "id".to_string(),
                column_type: "int8",
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
            original_refs: vec!["b_id.id".to_string()],
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
            build_delete_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(&sql_str, "DELETE FROM\n  a_table\nUSING\n  b_table\nWHERE (\n  b_table.id = $1 AND\n  a_table.b_id = b_table.id\n);");
        assert_eq!(
            prepared_values,
            vec![ColumnTypeValue::BigInt(ColumnValue::NotNullable(1))]
        );
    }
}
