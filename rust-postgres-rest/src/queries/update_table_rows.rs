use super::{
    foreign_keys::{fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::TypedColumnValue,
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        conditions_params_to_ast, generate_query_result_from_db, get_columns_str,
        get_db_column_str, get_where_string, validate_alias_identifier, validate_table_name,
        validate_where_column,
    },
    QueryResult,
};
use crate::{get_stats_cache_addr, Config, Error};
use futures::future::{err, Either, Future};
use lazy_static::lazy_static;
use rayon::prelude::*;
use regex::Regex;
use serde_json::{Map, Value as JsonValue};
use sqlparser::ast::Expr;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

lazy_static! {
    // check for strings
    static ref STRING_RE: Regex = Regex::new(r#"^['"](.+)['"]$"#).unwrap();
}

#[derive(Debug)]
/// Options used to execute an `UPDATE` SQL statement.
pub struct UpdateParams {
    /// A JSON object whose key-values represent column names and the values to set.
    pub column_values: Map<String, JsonValue>,
    /// WHERE expression.
    pub conditions: Option<String>,
    /// List of (foreign key) columns whose values are returned.
    pub returning_columns: Option<Vec<String>>,
    // Name of table to update.
    pub table: String,
}

/// Runs an UPDATE query on the selected table rows.
pub fn update_table_rows<T>(
    config: &Config<T>,
    params: UpdateParams,
) -> impl Future<Item = QueryResult, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
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

    // WHERE clause w/ foreign key references
    let where_ast = match conditions_params_to_ast(&params.conditions) {
        Ok(ast) => ast,
        Err(e) => return Either::A(err(e)),
    };
    column_expr_strings.par_extend(fk_columns_from_where_ast(&where_ast));

    // RETURNING column foreign key references
    let mut is_return_rows = false;
    let returning_column_strs;
    if let Some(columns) = &params.returning_columns {
        let returning_column_strs_result = columns
            .par_iter()
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
        column_expr_strings.par_extend(returning_column_strs);
    }

    // get table stats for building query (we need to know the column types)
    let table_clone = params.table.clone();
    let stats_future = config
        .connect()
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
    let config_clone = config.clone();
    let config_clone_2 = config.clone();

    let fk_future = stats_future
        .join(ForeignKeyReference::from_query_columns(
            config_clone,
            Arc::new(get_stats_cache_addr()),
            params.table.clone(),
            column_expr_strings,
        ))
        .and_then(move |(stats, fk_columns)| {
            let (statement_str, prepared_values) =
                match build_update_statement(params, stats, fk_columns, where_ast) {
                    Ok((stmt, prep_vals)) => (stmt, prep_vals),
                    Err(e) => return Either::A(err(e)),
                };

            let update_rows_future = generate_query_result_from_db(
                config_clone_2,
                statement_str,
                prepared_values,
                is_return_rows,
            );

            Either::B(update_rows_future)
        });

    Either::B(fk_future)
}

/// Returns the UPDATE query statement string and a vector of prepared values.
fn build_update_statement(
    params: UpdateParams,
    stats: Vec<TableColumnStat>,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: Expr,
) -> Result<(String, Vec<TypedColumnValue>), Error> {
    let mut query_str_arr = vec!["UPDATE ", &params.table, " SET "];
    let mut prepared_statement_values = vec![];
    let mut prepared_value_pos: usize = 1;
    let column_types: HashMap<String, &'static str> =
        TableColumnStat::stats_to_column_types(stats.clone());

    // Convert JSON object and append query_str and prepared_statement_values with column values
    let mut column_name_tokens_vec: Vec<Vec<&str>> = vec![];
    let mut set_prepared_values: Vec<String> = vec![];
    for (col, val) in params.column_values.iter() {
        let column_type = column_types
            .get(col)
            .ok_or_else(|| Error::generate_error("TABLE_COLUMN_TYPE_NOT_FOUND", col.clone()))?;

        // pretty sure function in a loop is a zero-cost abstraction?
        let mut append_prepared_value = |val: &JsonValue| -> Result<(), Error> {
            let val = TypedColumnValue::from_json(column_type, &val)?;
            prepared_statement_values.push(val);

            let actual_column_tokens = get_db_column_str(col, &params.table, &fks, false, false)?;

            column_name_tokens_vec.push(actual_column_tokens);
            set_prepared_values.push(format!("${}", prepared_value_pos));
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

                column_name_tokens_vec.push(actual_column_tokens);
                set_prepared_values.push(actual_value_tokens.join(""));
            }
        } else {
            append_prepared_value(&val)?;
        }
    }

    let column_values_len = params.column_values.len();
    for (i, (column_name_tokens, set_prepared_value)) in column_name_tokens_vec
        .into_iter()
        .zip(set_prepared_values.iter())
        .enumerate()
    {
        query_str_arr.par_extend(column_name_tokens);
        query_str_arr.push(" = ");
        query_str_arr.push(set_prepared_value);

        if i < column_values_len - 1 {
            query_str_arr.push(", ");
        }
    }

    // FROM string
    let from_tables_str = if !fks.is_empty() {
        ForeignKeyReference::join_foreign_key_references(
            &fks,
            |(_, _, referred_table, _)| referred_table.to_string(),
            ", ",
        )
    } else {
        "".to_string()
    };
    if &from_tables_str != "" {
        query_str_arr.push(" FROM ");
        query_str_arr.push(&from_tables_str);
    }

    // building WHERE string
    let fk_where_filter = if !fks.is_empty() {
        ForeignKeyReference::join_foreign_key_references(
            &fks,
            |(referring_table, referring_column, fk_table, fk_column)| {
                format!(
                    "{}.{} = {}.{}",
                    referring_table, referring_column, fk_table, fk_column
                )
            },
            " AND\n  ",
        )
    } else {
        String::from("")
    };

    let (mut where_string, where_column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    if &where_string != "" || &fk_where_filter != "" {
        query_str_arr.push("\nWHERE (\n  ");

        if &where_string != "" {
            // parse through the `WHERE` AST and return a tuple: (expression-with-prepared-params
            // string, Vec of tuples (position, Value)).
            let (where_string_with_prepared_positions, prepared_values_vec) =
                TypedColumnValue::generate_prepared_statement_from_ast_expr(
                    &where_ast,
                    &params.table,
                    &where_column_types,
                    Some(&mut prepared_value_pos),
                )?;
            where_string = where_string_with_prepared_positions;
            prepared_statement_values.par_extend(prepared_values_vec);

            query_str_arr.push(&where_string);

            if fk_where_filter != "" {
                query_str_arr.push(" AND\n  ");
            }
        }

        if fk_where_filter != "" {
            query_str_arr.push(&fk_where_filter);
        }

        query_str_arr.push("\n)");
    }

    // returning_columns
    if let Some(returned_column_names) = &params.returning_columns {
        query_str_arr.push("\nRETURNING\n  ");

        let returning_columns_str = get_columns_str(returned_column_names, &params.table, &fks)?;
        query_str_arr.par_extend(returning_columns_str);
    }

    query_str_arr.push(";");

    Ok((query_str_arr.join(""), prepared_statement_values))
}

#[cfg(test)]
mod build_update_statement_tests {
    use super::*;
    use crate::queries::postgres_types::IsNullColumnValue;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn simple() {
        let params = UpdateParams {
            column_values: json!({"name": "'test'"}).as_object().unwrap().clone(),
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
            build_update_statement(params, stats, fks, Expr::Identifier("".to_string())).unwrap();

        assert_eq!(&sql_str, "UPDATE a_table SET name = $1;");
        assert_eq!(
            prepared_values,
            vec![TypedColumnValue::Text(IsNullColumnValue::NotNullable(
                "test".to_string()
            ))]
        );
    }

    #[test]
    fn fk_returning_columns() {
        let conditions = "id = 2";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();
        let params = UpdateParams {
            column_values: json!({"nemesis_name": "nemesis_id.name"})
                .as_object()
                .unwrap()
                .clone(),
            conditions: Some(conditions.to_string()),
            returning_columns: Some(vec!["id".to_string(), "nemesis_name".to_string()]),
            table: "throne".to_string(),
        };
        let stats = vec![
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
                column_name: "nemesis_id".to_string(),
                column_type: "int8",
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("adult".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8"),
                char_max_length: None,
                char_octet_length: None,
            },
            TableColumnStat {
                column_name: "nemesis_name".to_string(),
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
                column_name: "house".to_string(),
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
                column_name: "ruler".to_string(),
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
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["nemesis_id.name".to_string()],
            referring_table: "throne".to_string(),
            referring_column: "nemesis_id".to_string(),
            referring_column_type: "int8",
            foreign_key_table: "adult".to_string(),
            foreign_key_table_stats: vec![],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
            nested_fks: vec![],
        }];

        let (sql_str, prepared_values) =
            build_update_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(
            &sql_str,
            "UPDATE throne SET nemesis_name = adult.name FROM adult\nWHERE (\n  throne.id = $1 AND\n  throne.nemesis_id = adult.id\n)\nRETURNING\n  throne.id AS \"id\", throne.nemesis_name AS \"nemesis_name\";"
        );
        assert_eq!(
            prepared_values,
            vec![TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(2))]
        );
    }

    #[test]
    fn fk_returning_columns_no_conditions() {
        let params = UpdateParams {
            column_values: json!({"nemesis_name": "nemesis_id.name"})
                .as_object()
                .unwrap()
                .clone(),
            conditions: None,
            returning_columns: Some(vec!["id".to_string(), "nemesis_name".to_string()]),
            table: "throne".to_string(),
        };
        let stats = vec![
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
                column_name: "nemesis_id".to_string(),
                column_type: "int8",
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("adult".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8"),
                char_max_length: None,
                char_octet_length: None,
            },
            TableColumnStat {
                column_name: "nemesis_name".to_string(),
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
                column_name: "house".to_string(),
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
                column_name: "ruler".to_string(),
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
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["nemesis_id.name".to_string()],
            referring_table: "throne".to_string(),
            referring_column: "nemesis_id".to_string(),
            referring_column_type: "int8",
            foreign_key_table: "adult".to_string(),
            foreign_key_table_stats: vec![],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
            nested_fks: vec![],
        }];

        let (sql_str, prepared_values) =
            build_update_statement(params, stats, fks, Expr::Identifier("".to_string())).unwrap();

        assert_eq!(
            &sql_str,
            "UPDATE throne SET nemesis_name = adult.name FROM adult\nWHERE (\n  throne.nemesis_id = adult.id\n)\nRETURNING\n  throne.id AS \"id\", throne.nemesis_name AS \"nemesis_name\";"
        );
        assert_eq!(prepared_values, vec![]);
    }

    #[test]
    fn nested_fk_returning_columns() {
        let conditions = "id = 1";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();
        let params = UpdateParams {
            column_values: json!({"name": "team_id.coach_id.name"})
                .as_object()
                .unwrap()
                .clone(),
            conditions: Some(conditions.to_string()),
            returning_columns: Some(vec!["id".to_string(), "team_id.coach_id.name".to_string()]),
            table: "player".to_string(),
        };
        let stats = vec![
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
                column_name: "team_id".to_string(),
                column_type: "int8",
                default_value: None,
                is_nullable: true,
                is_foreign_key: true,
                foreign_key_table: Some("team".to_string()),
                foreign_key_column: Some("id".to_string()),
                foreign_key_column_type: Some("int8"),
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
        ];
        let fks = vec![ForeignKeyReference {
            original_refs: vec!["team_id.coach_id.name".to_string()],
            referring_table: "player".to_string(),
            referring_column: "team_id".to_string(),
            referring_column_type: "int8",
            foreign_key_table: "team".to_string(),
            foreign_key_table_stats: vec![],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["coach_id.name".to_string()],
                referring_table: "team".to_string(),
                referring_column: "coach_id".to_string(),
                referring_column_type: "int8",
                foreign_key_table: "coach".to_string(),
                foreign_key_table_stats: vec![],
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8",
                nested_fks: vec![],
            }],
        }];

        let (sql_str, prepared_values) =
            build_update_statement(params, stats, fks, where_ast).unwrap();

        assert_eq!(
            &sql_str,
            "UPDATE player SET name = coach.name FROM team, coach\nWHERE (\n  player.id = $1 AND\n  player.team_id = team.id AND\n  team.coach_id = coach.id\n)\nRETURNING\n  player.id AS \"id\", coach.name AS \"team_id.coach_id.name\";"
        );
        assert_eq!(
            prepared_values,
            vec![TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(1))]
        );
    }
}
