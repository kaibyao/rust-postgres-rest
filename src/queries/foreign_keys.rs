use super::table_stats::get_column_stats;
use crate::db::Connection;
use crate::errors::ApiError;
use sqlparser::{
    dialect::PostgreSqlDialect,
    sqlast::{ASTNode, SQLQuery, SQLSelect, SQLSetExpr, SQLStatement},
    sqlparser::Parser,
};
use std::borrow::BorrowMut;
use std::collections::HashMap;

// TODO: write tests for where_clause_str_to_ast
// TODO: write tests for fk_columns_from_where_ast

/// Converts a WHERE clause string into an ASTNode.
pub fn where_clause_str_to_ast(clause: &str) -> Result<Option<ASTNode>, ApiError> {
    let full_statement = ["SELECT * FROM a_table WHERE ", clause].join("");
    let dialect = PostgreSqlDialect {};

    // convert the statement into an AST, and then extract the "WHERE" portion of the AST
    let mut parsed = Parser::parse_sql(&dialect, full_statement)?;
    let statement_ast = parsed.remove(0);

    if let SQLStatement::SQLSelect(SQLQuery {
        body: query_body, ..
    }) = statement_ast
    {
        return Ok(extract_where_ast_from_sqlsetexpr(query_body));
    }

    Ok(None)
}

/// Finds and returns the ASTNode that represents the WHERE clause of a SELECT statement
fn extract_where_ast_from_sqlsetexpr(expr: SQLSetExpr) -> Option<ASTNode> {
    match expr {
        SQLSetExpr::Query(boxed_sql_query) => {
            extract_where_ast_from_sqlsetexpr(boxed_sql_query.body)
        }
        SQLSetExpr::Select(SQLSelect {
            selection: where_ast_opt,
            ..
        }) => where_ast_opt,
        SQLSetExpr::SetOperation { .. } => unimplemented!("Set operations not supported"),
    }
}

/// Extracts the raw/incorrect foreign key column strings from a WHERE ASTNode, as well as a HashMap of column string:&ASTNode.
pub fn fk_columns_from_where_ast(
    ast: &mut ASTNode,
) -> Vec<(String, &mut ASTNode)> {
    let mut fks = vec![];

    match ast {
        ASTNode::SQLQualifiedWildcard(wildcard_vec) => {
            fks.push((wildcard_vec.join("."), ast));
        }
        ASTNode::SQLCompoundIdentifier(nested_fk_column_vec) => {
            fks.push((nested_fk_column_vec.join("."), ast));
        }
        ASTNode::SQLIsNull(null_ast_box) => {
            fks.extend(fk_columns_from_where_ast(null_ast_box.borrow_mut()));
        }
        ASTNode::SQLIsNotNull(null_ast_box) => {
            fks.extend(fk_columns_from_where_ast(null_ast_box.borrow_mut()));
        }
        ASTNode::SQLInList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(list_expr_ast_box_ref.borrow_mut()));

            for list_ast in list_ast_vec {
                fks.extend(fk_columns_from_where_ast(list_ast));
            }
        }
        ASTNode::SQLBinaryExpr {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(bin_left_ast_box_ref.borrow_mut()));
            fks.extend(fk_columns_from_where_ast(bin_right_ast_box_ref.borrow_mut()));
        }
        ASTNode::SQLCast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(cast_expr_box_ref.borrow_mut()));
        }
        ASTNode::SQLNested(nested_ast_box_ref) => {
            fks.extend(fk_columns_from_where_ast(nested_ast_box_ref.borrow_mut()));
        }
        ASTNode::SQLUnary {
            expr: unary_expr_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(unary_expr_box_ref.borrow_mut()));
        }
        ASTNode::SQLBetween {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(between_expr_ast_box_ref.borrow_mut()));
            fks.extend(fk_columns_from_where_ast(between_low_ast_box_ref.borrow_mut()));
            fks.extend(fk_columns_from_where_ast(between_high_ast_box_ref.borrow_mut()));
        }
        ASTNode::SQLFunction {
            args: args_ast_vec, ..
        } => {
            for ast_arg in args_ast_vec {
                fks.extend(fk_columns_from_where_ast(ast_arg));
            }
        }
        ASTNode::SQLCase {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                fks.extend(fk_columns_from_where_ast(case_condition_ast));
            }

            for case_results_ast_vec in case_results_ast_vec {
                fks.extend(fk_columns_from_where_ast(case_results_ast_vec));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                fks.extend(fk_columns_from_where_ast(case_else_results_ast_box.borrow_mut()));
            }
        }
        // below is unsupported
        ASTNode::SQLIdentifier(_non_nested_column_name) => (),
        ASTNode::SQLWildcard => (),
        ASTNode::SQLInSubquery { .. } => (), // subqueries in WHERE statement are not supported
        ASTNode::SQLValue(_val) => (),
        ASTNode::SQLSubquery(_query_box) => (), // subqueries in WHERE are not supported
    };

    fks
}

/// Represents a single foreign key, usually generated by a queried column using dot-syntax.
pub struct ForeignKeyReference {
    /// The original column strings referencing a (possibly nested) foreign key value.
    pub original_refs: Vec<String>,

    /// The parent table name that contains the foreign key column.
    pub referring_table: String,

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
                            referring_table: table.to_string(),
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
                    referring_table: table.to_string(),
                    table_referred: foreign_key_table,
                    table_column_referred: stat.foreign_key_columns.unwrap_or_else(String::new),
                    nested_fks: None,
                    original_refs,
                }))
            })
            .collect();

        Ok(Some(filtered_stats_result?))
    }

    /// Given an array of ForeignKeyReference, find the one that matches a given table and column name.
    pub fn find<'a>(refs: &'a [Self], table: &str, col: &'a str) -> Option<&'a Self> {
        for fkr in refs {
            if fkr.referring_table != table {
                continue;
            }

            let found_orig_ref = fkr.original_refs.iter().find(|ref_col| col == *ref_col);

            if found_orig_ref.is_some() {
                return Some(fkr);
            }

            if let Some(nested_fks) = &fkr.nested_fks {
                return Self::find(nested_fks, table, col);
            }
        }

        None
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
