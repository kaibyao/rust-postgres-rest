use super::{
    foreign_keys::{fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::{convert_row_fields, ColumnTypeValue, RowFields},
    query_types::{QueryParamsUpdate, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        get_columns_str, get_db_column_str, get_where_string, validate_alias_identifier,
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
use serde_json::Value as JsonValue;
use sqlparser::ast::Expr;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::types::ToSql;

lazy_static! {
    // check for strings
    static ref STRING_RE: Regex = Regex::new(r#"^['"](.+)['"]$"#).unwrap();
}

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
    let (columns, values): (Vec<&String>, Vec<&JsonValue>) = params.column_values.iter().unzip();

    let mut column_expr_strings: Vec<String> = match columns
        .into_iter()
        .map(|col| -> Result<String, Error> {
            match validate_alias_identifier(col)? {
                Some((actual_column_ref, _alias)) => Ok(actual_column_ref.to_string()),
                _ => Ok(col.to_string()),
            }
        })
        .collect::<Result<Vec<String>, Error>>()
    {
        Ok(column_expr_strings) => column_expr_strings,
        Err(e) => return Either::A(err(e)),
    };

    // search for column expression values and append to column_expr_strings
    for val in values {
        // check for expression used as a column value
        if let Some(val_str) = val.as_str() {
            if !STRING_RE.is_match(val_str) {
                if let Err(e) = validate_where_column(val_str) {
                    return Either::A(err(e));
                }

                // column value is an expression, append to column_expr_strings
                column_expr_strings.push(val_str.to_string());
            }
        }
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
    column_expr_strings.extend(fk_columns_from_where_ast(&where_ast));

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

    // dbg!(&column_expr_strings);

    let fk_future = stats_future
        .join(ForeignKeyReference::from_query_columns(
            state.config.db_url,
            Arc::new(addr_clone),
            params.table.clone(),
            column_expr_strings,
        ))
        .and_then(move |(stats, fk_columns)| {
            let (statement_str, prepared_values) =
                match build_update_statement(params, stats, fk_columns, where_ast) {
                    Ok((stmt, prep_vals)) => (stmt, prep_vals),
                    Err(e) => return Either::A(err(e)),
                };

            let update_rows_future =
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

            Either::B(update_rows_future)
        });

    Either::B(fk_future)
}

/// Returns the UPDATE query statement string and a vector of prepared values.
fn build_update_statement(
    params: QueryParamsUpdate,
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
    let column_types: HashMap<String, String> =
        TableColumnStat::stats_to_column_types(stats.clone());

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

            let actual_column_tokens = get_db_column_str(col, &params.table, &fks, false, false)?;

            query_str_arr.push(actual_column_tokens.join(""));

            // todo: experiment moving this out of the loop using 2 vecs. convert String -> &str
            let prepared_value_pos_str = prepared_value_pos.to_string();
            query_str_arr.push([" = $", &prepared_value_pos_str].join(""));
            prepared_value_pos_vec.push(prepared_value_pos_str);
            prepared_value_pos += 1;

            Ok(())
        };

        // check for expression used as a column value
        if let Some(val_str) = val.as_str() {
            if STRING_RE.is_match(val_str) {
                // column value is a string
                let captures = STRING_RE.captures(val_str).unwrap();
                let val_string = captures.get(1).unwrap().as_str().to_string();
                let val = JsonValue::String(val_string);

                append_prepared_value(&val)?;
            } else {
                // column value is an expression, parse foreign key usage
                let actual_column_tokens =
                    get_db_column_str(col, &params.table, &fks, false, false)?;
                let actual_value_tokens =
                    get_db_column_str(val_str, &params.table, &fks, false, true)?;

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

    // dbg!(&fks);

    // FROM string
    if !fks.is_empty() {
        query_str_arr.push(" FROM ".to_string());
        let from_tables_str = ForeignKeyReference::join_foreign_key_references(
            &fks,
            |(_, _, referred_table, _)| referred_table.to_string(),
            ", ",
        );
        query_str_arr.push(from_tables_str);
    }

    // building WHERE string
    let (mut where_string, where_column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    if &where_string != "" {
        query_str_arr.push(" WHERE (".to_string());

        // parse through the `WHERE` AST and return a tuple: (expression-with-prepared-params
        // string, Vec of tuples (position, Value)).
        let (where_string_with_prepared_positions, prepared_values_vec) =
            ColumnTypeValue::generate_prepared_statement_from_ast_expr(
                &where_ast,
                &params.table,
                &where_column_types,
                Some(&mut prepared_value_pos),
            )?;
        where_string = where_string_with_prepared_positions;
        prepared_statement_values.extend(prepared_values_vec);

        query_str_arr.push(where_string);
        query_str_arr.push(")".to_string());
    }

    // returning_columns
    if let Some(returned_column_names) = params.returning_columns {
        query_str_arr.push(" RETURNING ".to_string());

        let returning_columns_str = get_columns_str(&returned_column_names, &params.table, &fks)?;
        query_str_arr.push(returning_columns_str.join(""));
    }

    query_str_arr.push(";".to_string());
    // dbg!(query_str_arr.join(""));

    Ok((query_str_arr.join(""), prepared_statement_values))
}

#[cfg(test)]
mod build_update_statement_tests {
    use super::*;
    use crate::queries::{postgres_types::ColumnValue, query_types::QueryParamsUpdate};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn fk_returning_columns() {
        let conditions = "id = 2".to_string();
        let where_ast = where_clause_str_to_ast(&conditions).unwrap().unwrap();
        let params = QueryParamsUpdate {
            column_values: json!({"nemesis_name": "nemesis_id.name"})
                .as_object()
                .unwrap()
                .clone(),
            conditions: Some(conditions),
            returning_columns: Some(vec!["id".to_string(), "nemesis_name".to_string()]),
            table: "throne".to_string(),
        };
        let stats = vec![
            TableColumnStat {
                column_name: "id".to_string(),
                column_type: "int8".to_string(),
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
                column_name: "nemesis_id".to_string(),
                column_type: "int8".to_string(),
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("adult".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8".to_string()),
                char_max_length: None,
                char_octet_length: None,
            },
            TableColumnStat {
                column_name: "nemesis_name".to_string(),
                column_type: "text".to_string(),
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
                column_name: "house".to_string(),
                column_type: "text".to_string(),
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
                column_name: "ruler".to_string(),
                column_type: "text".to_string(),
                default_value: None,
                is_nullable: true,
                is_foreign_key: false,
                foreign_key_table: None,
                foreign_key_column: None,
                foreign_key_column_type: None,
                char_max_length: None,
                char_octet_length: None,
            },
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["nemesis_id.name".to_string()],
            referring_table: "throne".to_string(),
            referring_column: "nemesis_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![],
        }];

        let (sql_str, prepared_values) =
            build_update_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(
            &sql_str,
            "UPDATE throne SET nemesis_name = adult.name FROM adult WHERE (throne.id = $1);"
        );
        assert_eq!(
            prepared_values,
            vec![ColumnTypeValue::BigInt(ColumnValue::NotNullable(2))]
        );
    }

    #[test]
    fn nested_fk_returning_columns() {
        let conditions = "id = 1".to_string();
        let where_ast = where_clause_str_to_ast(&conditions).unwrap().unwrap();
        let params = QueryParamsUpdate {
            column_values: json!({"name": "team_id.coach_id.name"})
                .as_object()
                .unwrap()
                .clone(),
            conditions: Some(conditions),
            returning_columns: Some(vec!["id".to_string(), "team_id.coach_id.name".to_string()]),
            table: "player".to_string(),
        };
        let stats = vec![
            TableColumnStat {
                column_name: "id".to_string(),
                column_type: "int8".to_string(),
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
                column_name: "team_id".to_string(),
                column_type: "int8".to_string(),
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("team".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8".to_string()),
                char_max_length: None,
                char_octet_length: None,
            },
            TableColumnStat {
                column_name: "name".to_string(),
                column_type: "text".to_string(),
                default_value: None,
                is_nullable: true,
                is_foreign_key: false,
                foreign_key_table: None,
                foreign_key_column: None,
                foreign_key_column_type: None,
                char_max_length: None,
                char_octet_length: None,
            },
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["team_id.coach_id.name".to_string()],
            referring_table: "player".to_string(),
            referring_column: "team_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "team".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["coach_id.name".to_string()],
                referring_table: "team".to_string(),
                referring_column: "coach_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "coach".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];

        let (sql_str, prepared_values) =
            build_update_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(
            &sql_str,
            "UPDATE player SET name = coach.name FROM team, coach WHERE (player.id = $1);"
        );
        assert_eq!(
            prepared_values,
            vec![ColumnTypeValue::BigInt(ColumnValue::NotNullable(1))]
        );
    }
}
