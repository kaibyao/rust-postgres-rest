use super::table_stats::get_column_stats;
use crate::db::Connection;
use crate::errors::ApiError;
use sqlparser::{
    dialect::PostgreSqlDialect,
    sqlast::{ASTNode, SQLQuery, SQLSelect, SQLSetExpr, SQLStatement},
    sqlparser::Parser,
};
use std::collections::HashMap;

/// Converts a WHERE clause string into a vector of foreign key column strings.
pub fn convert_where_clause_str_to_fk_columns(
    clause: &str,
) -> Result<Option<Vec<String>>, ApiError> {
    let full_statement = ["SELECT * FROM a_table WHERE ", clause].join("");
    let dialect = PostgreSqlDialect {};
    let ast = &Parser::parse_sql(&dialect, full_statement)?[0];

    let fk_columns_opt = match ast {
        SQLStatement::SQLSelect(SQLQuery {
            body: query_body, ..
        }) => {
            // parse the main body of the SELECT statement
            let fk_columns = extract_fk_columns_from_sql_query_where(query_body, None);
            Some(fk_columns)
        }
        SQLStatement::SQLInsert { .. } => {
            // unimplemented!("There is no WHERE clause in an insert statement.")
            None
        }
        SQLStatement::SQLUpdate { .. } => None, // unimplemented!("To be finished later.")
        SQLStatement::SQLDelete { .. } => None, // unimplemented!("To be finished later.")
        _ => None,                              // unimplemented!("Functionality not implemented.")
    };

    Ok(fk_columns_opt)
}

fn extract_fk_columns_from_sql_query_where(
    expr: &SQLSetExpr,
    fk_columns_accumulate: Option<Vec<String>>,
) -> Vec<String> {
    let mut fk_columns = if let Some(accumulated_fk_columns) = fk_columns_accumulate {
        accumulated_fk_columns
    } else {
        vec![]
    };

    match expr {
        SQLSetExpr::Query(boxed_sql_query) => {
            fk_columns = extract_fk_columns_from_sql_query_where(
                &boxed_sql_query.as_ref().body,
                Some(fk_columns),
            );
        }
        SQLSetExpr::Select(SQLSelect {
            selection: where_ast_opt,
            ..
        }) => {
            if let Some(where_ast) = where_ast_opt {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(where_ast)));
            }
        }
        SQLSetExpr::SetOperation { .. } => (), // Set operations not supported
    };

    fk_columns
}

fn push_nested_ast_fk_columns_to_vec(ast_opt: Option<&ASTNode>) -> Vec<String> {
    let mut fk_columns = vec![];

    let ast = if let Some(ast_node) = ast_opt {
        ast_node
    } else {
        return fk_columns;
    };

    match ast {
        ASTNode::SQLIdentifier(_non_nested_column_name) => (),
        ASTNode::SQLWildcard => (),
        ASTNode::SQLQualifiedWildcard(wildcard_vec) => {
            fk_columns.push(wildcard_vec.join("."));
        }
        ASTNode::SQLCompoundIdentifier(nested_fk_column_vec) => {
            fk_columns.push(nested_fk_column_vec.join("."));
        }
        ASTNode::SQLIsNull(null_ast_box) => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                null_ast_box.as_ref(),
            )));
        }
        ASTNode::SQLIsNotNull(null_ast_box) => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                null_ast_box.as_ref(),
            )));
        }
        ASTNode::SQLInList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                list_expr_ast_box_ref.as_ref(),
            )));

            for list_ast in list_ast_vec {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(list_ast)));
            }
        }
        ASTNode::SQLInSubquery { .. } => (), // subqueries in WHERE statement are not supported
        ASTNode::SQLBetween {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                between_expr_ast_box_ref.as_ref(),
            )));
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                between_low_ast_box_ref.as_ref(),
            )));
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                between_high_ast_box_ref.as_ref(),
            )));
        }
        ASTNode::SQLBinaryExpr {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                bin_left_ast_box_ref.as_ref(),
            )));
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                bin_right_ast_box_ref.as_ref(),
            )));
        }
        ASTNode::SQLCast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                cast_expr_box_ref.as_ref(),
            )));
        }
        ASTNode::SQLNested(nested_ast_box_ref) => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                nested_ast_box_ref.as_ref(),
            )));
        }
        ASTNode::SQLUnary {
            expr: unary_expr_box_ref,
            ..
        } => {
            fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                unary_expr_box_ref.as_ref(),
            )));
        }
        ASTNode::SQLValue(_val) => (),
        ASTNode::SQLFunction {
            args: args_ast_vec, ..
        } => {
            for ast_arg in args_ast_vec {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(ast_arg)));
            }
        }
        ASTNode::SQLCase {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(case_condition_ast)));
            }

            for case_results_ast_vec in case_results_ast_vec {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                    case_results_ast_vec,
                )));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                fk_columns.extend(push_nested_ast_fk_columns_to_vec(Some(
                    case_else_results_ast_box.as_ref(),
                )));
            }
        }
        ASTNode::SQLSubquery(_query_box) => (), // subqueries in WHERE are not supported
    };

    fk_columns
}

/// Represents a single foreign key, usually generated by a queried column using dot-syntax.
pub struct ForeignKeyReference {
    /// The original column strings referencing a (possibly nested) foreign key value.
    pub original_refs: Vec<String>,

    /// The parent table’s column name that is the foreign key.
    pub referring_column: String,

    /// The table being referred by the foreign key.
    pub table_referred: String,

    /// The column of the table being referred by the foreign key.
    pub table_column_referred: String,

    /// Any child foreign key columns that are part of the original_ref string.
    pub nested_fks: Option<Vec<ForeignKeyReference>>,
}

impl ForeignKeyReference {
    /// Given a table name and list of table column names, return a list of foreign key references. If none of the provided columns are foreign keys, returns `Ok(None)`.
    ///
    /// # Examples
    ///
    /// ## Simple query (1 level deep)
    ///
    /// ```
    /// // a_table.a_foreign_key references b_table.id
    /// // a_table.another_foreign_key references c_table.id
    ///
    /// assert_eq!(
    ///     get_foreign_keys_from_query_columns(
    ///         conn,
    ///         "a_table",
    ///         &[
    ///             "a_foreign_key.some_text",
    ///             "another_foreign_key.some_str",
    ///             "b"
    ///         ]
    ///     ),
    ///     Ok(Some(vec![
    ///         ForeignKeyReference {
    ///             referring_column: "a_foreign_key".to_string(),
    ///             table_referred: "b_table".to_string(),
    ///             table_column_referred: "id".to_string(),
    ///             nested_fks: None,
    ///         },
    ///         ForeignKeyReference {
    ///             referring_column: "another_foreign_key".to_string(),
    ///             table_referred: "c_table".to_string(),
    ///             table_column_referred: "id".to_string(),
    ///             nested_fks: None,
    ///         }
    ///     ]))
    /// );
    /// ```
    ///
    /// ## Nested foreign keys
    ///
    /// ```
    /// // a_foreign_key references b_table.id
    /// // another_foreign_key references c_table.id
    /// // another_foreign_key.nested_fk references d_table.id
    /// // another_foreign_key.different_nested_fk references e_table.id
    ///
    /// assert_eq!(
    ///     get_foreign_keys_from_query_columns(
    ///         conn,
    ///         "a_table",
    ///         &[
    ///             "a_foreign_key.some_text",
    ///             "another_foreign_key.nested_fk.some_str",
    ///             "another_foreign_key.different_nested_fk.some_int",
    ///             "b"
    ///         ]
    ///     ),
    ///     Ok(Some(vec![
    ///       ForeignKeyReference {
    ///           referring_column: "a_foreign_key".to_string(),
    ///           table_referred: "b_table".to_string(),
    ///           table_column_referred: "id".to_string(),
    ///           nested_fks: None
    ///       },
    ///       ForeignKeyReference {
    ///           referring_column: "another_foreign_key".to_string(),
    ///           table_referred: "b_table".to_string(),
    ///           table_column_referred: "id".to_string(),
    ///           nested_fks: Some(vec![
    ///               ForeignKeyReference {
    ///                   referring_column: "nested_fk".to_string(),
    ///                   table_referred: "d_table".to_string(),
    ///                   table_column_referred: "id".to_string(),
    ///                   nested_fks: None
    ///               },
    ///               ForeignKeyReference {
    ///                   referring_column: "different_nested_fk".to_string(),
    ///                   table_referred: "e_table".to_string(),
    ///                   table_column_referred: "id".to_string(),
    ///                   nested_fks: None
    ///               }
    ///           ])
    ///       }
    ///     ]))
    /// );
    /// ```
    pub fn from_query_columns(
        conn: &Connection,
        table: &str,
        columns: &[&str],
    ) -> Result<Option<Vec<Self>>, ApiError> {
        let mut fk_columns: Vec<String> = columns
            .iter()
            .filter_map(|col| {
                if col.contains('.') {
                    Some(col.to_string())
                } else {
                    None
                }
            })
            .collect();
        fk_columns.sort_unstable();
        fk_columns.dedup();

        // First, check if any columns are using the `.` foreign key delimiter.
        if fk_columns.is_empty() {
            return Ok(None);
        }

        // group FKs & original column references by the column being referenced
        let mut fk_columns_grouped: HashMap<&str, (Vec<&str>, Vec<&str>)> = HashMap::new();
        // need to somehow get col into the map
        for col in fk_columns.iter() {
            if let Some(dot_index) = col.find('.') {
                if let (Some(parent_col_name), Some(child_column)) =
                    (col.get(0..dot_index), col.get(dot_index..))
                {
                    if !fk_columns_grouped.contains_key(parent_col_name) {
                        fk_columns_grouped.insert(parent_col_name, (vec![child_column], vec![col]));
                    } else {
                        let (child_columns, original_refs) =
                            fk_columns_grouped.get_mut(parent_col_name).unwrap();

                        child_columns.push(child_column);
                        original_refs.push(col);
                    }
                }
            }
        }

        // get column stats for table
        let stats = get_column_stats(conn, table)?;

        // filter stats to just the ones that match given columns and return the formatted data
        let filtered_stats_result: Result<Vec<Self>, ApiError> = stats
            .into_iter()
            .filter_map(|stat| -> Option<Result<Self, ApiError>> {
                if !stat.is_foreign_key {
                    return None;
                }

                // find matching column and child columns that belong to the same referenced table
                let (_parent_col_match, child_col_vec_match, original_refs_match) =
                    match fk_columns_grouped
                        .iter()
                        .find(|(&parent_col, _child_col_vec)| parent_col == stat.column_name)
                    {
                        Some((
                            matched_parent_fk_column,
                            (matched_child_col_vec, matched_orig_refs),
                        )) => (
                            matched_parent_fk_column,
                            matched_child_col_vec,
                            matched_orig_refs,
                        ),
                        None => return None,
                    };
                let original_refs = original_refs_match
                    .iter()
                    .map(|col| col.to_string())
                    .collect();
                let foreign_key_table = stat.foreign_key_table.unwrap();

                if child_col_vec_match
                    .iter()
                    .any(|&child_col| child_col.contains('.'))
                {
                    // child column is also an FK => recursively run this function

                    let nested_fk_result =
                        Self::from_query_columns(conn, &foreign_key_table, child_col_vec_match);

                    if let Err(e) = nested_fk_result {
                        return Some(Err(e));
                    } else if let Ok(Some(fk_result_vec)) = nested_fk_result {
                        return Some(Ok(ForeignKeyReference {
                            referring_column: stat.column_name,
                            table_referred: foreign_key_table,
                            table_column_referred: stat
                                .foreign_key_columns
                                .unwrap_or_else(String::new),
                            nested_fks: Some(fk_result_vec),
                            original_refs,
                        }));
                    }
                }

                // child column is not an FK (is a column) => return a QueriedForeignKeyResult::Reference
                Some(Ok(ForeignKeyReference {
                    referring_column: stat.column_name,
                    table_referred: foreign_key_table,
                    table_column_referred: stat.foreign_key_columns.unwrap_or_else(String::new),
                    nested_fks: None,
                    original_refs,
                }))
            })
            .collect();

        Ok(Some(filtered_stats_result?))
    }


    // /// Given a table name and list of foreign key references, construct the column and `INNER JOIN` SQL strings to be used in a query.
    // pub fn fk_reference_arr_to_sql(
    //     table: &str,
    //     columns: &[&str],
    //     fk_refs: &[Self],
    // ) -> (Vec<String>, String) {
    //     (vec![], "".to_string())
    // }
}
