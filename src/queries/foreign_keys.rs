use futures::future::{err, join_all, lazy, ok, Either, Future, FutureResult};
use sqlparser::{
    dialect::PostgreSqlDialect,
    sqlast::{ASTNode, SQLQuery, SQLSelect, SQLSetExpr, SQLStatement},
    sqlparser::Parser,
};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio_postgres::Client;

use super::select_table_stats::{
    select_column_stats, select_column_stats_statement, TableColumnStat,
};
use crate::errors::ApiError;

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

/// Extracts the foreign key ASTNodes froma WHERE ASTNode.
pub fn fk_ast_nodes_from_where_ast(ast: &mut ASTNode) -> Vec<(String, &mut ASTNode)> {
    let mut fks = vec![];

    match ast {
        ASTNode::SQLQualifiedWildcard(wildcard_vec) => {
            fks.push((wildcard_vec.join("."), ast));
        }
        ASTNode::SQLCompoundIdentifier(nested_fk_column_vec) => {
            fks.push((nested_fk_column_vec.join("."), ast));
        }
        ASTNode::SQLIsNull(null_ast_box) => {
            fks.extend(fk_ast_nodes_from_where_ast(null_ast_box.borrow_mut()));
        }
        ASTNode::SQLIsNotNull(null_ast_box) => {
            fks.extend(fk_ast_nodes_from_where_ast(null_ast_box.borrow_mut()));
        }
        ASTNode::SQLInList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fks.extend(fk_ast_nodes_from_where_ast(
                list_expr_ast_box_ref.borrow_mut(),
            ));

            for list_ast in list_ast_vec {
                fks.extend(fk_ast_nodes_from_where_ast(list_ast));
            }
        }
        ASTNode::SQLBinaryExpr {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fks.extend(fk_ast_nodes_from_where_ast(
                bin_left_ast_box_ref.borrow_mut(),
            ));
            fks.extend(fk_ast_nodes_from_where_ast(
                bin_right_ast_box_ref.borrow_mut(),
            ));
        }
        ASTNode::SQLCast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fks.extend(fk_ast_nodes_from_where_ast(cast_expr_box_ref.borrow_mut()));
        }
        ASTNode::SQLNested(nested_ast_box_ref) => {
            fks.extend(fk_ast_nodes_from_where_ast(nested_ast_box_ref.borrow_mut()));
        }
        ASTNode::SQLUnary {
            expr: unary_expr_box_ref,
            ..
        } => {
            fks.extend(fk_ast_nodes_from_where_ast(unary_expr_box_ref.borrow_mut()));
        }
        ASTNode::SQLBetween {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fks.extend(fk_ast_nodes_from_where_ast(
                between_expr_ast_box_ref.borrow_mut(),
            ));
            fks.extend(fk_ast_nodes_from_where_ast(
                between_low_ast_box_ref.borrow_mut(),
            ));
            fks.extend(fk_ast_nodes_from_where_ast(
                between_high_ast_box_ref.borrow_mut(),
            ));
        }
        ASTNode::SQLFunction {
            args: args_ast_vec, ..
        } => {
            for ast_arg in args_ast_vec {
                fks.extend(fk_ast_nodes_from_where_ast(ast_arg));
            }
        }
        ASTNode::SQLCase {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                fks.extend(fk_ast_nodes_from_where_ast(case_condition_ast));
            }

            for case_results_ast_vec in case_results_ast_vec {
                fks.extend(fk_ast_nodes_from_where_ast(case_results_ast_vec));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                fks.extend(fk_ast_nodes_from_where_ast(
                    case_else_results_ast_box.borrow_mut(),
                ));
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

/// Similar to `fk_ast_nodes_from_where_ast`. Extracts the raw/incorrect foreign key column strings from a WHERE ASTNode
pub fn fk_columns_from_where_ast(ast: &ASTNode) -> Vec<String> {
    let mut fks = vec![];

    match ast {
        ASTNode::SQLQualifiedWildcard(wildcard_vec) => {
            fks.push(wildcard_vec.join("."));
        }
        ASTNode::SQLCompoundIdentifier(nested_fk_column_vec) => {
            fks.push(nested_fk_column_vec.join("."));
        }
        ASTNode::SQLIsNull(null_ast_box) => {
            fks.extend(fk_columns_from_where_ast(null_ast_box.as_ref()));
        }
        ASTNode::SQLIsNotNull(null_ast_box) => {
            fks.extend(fk_columns_from_where_ast(null_ast_box.as_ref()));
        }
        ASTNode::SQLInList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(list_expr_ast_box_ref.as_ref()));

            for list_ast in list_ast_vec {
                fks.extend(fk_columns_from_where_ast(list_ast));
            }
        }
        ASTNode::SQLBinaryExpr {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(bin_left_ast_box_ref.as_ref()));
            fks.extend(fk_columns_from_where_ast(bin_right_ast_box_ref.as_ref()));
        }
        ASTNode::SQLCast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(cast_expr_box_ref.as_ref()));
        }
        ASTNode::SQLNested(nested_ast_box_ref) => {
            fks.extend(fk_columns_from_where_ast(nested_ast_box_ref.as_ref()));
        }
        ASTNode::SQLUnary {
            expr: unary_expr_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(unary_expr_box_ref.as_ref()));
        }
        ASTNode::SQLBetween {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fks.extend(fk_columns_from_where_ast(between_expr_ast_box_ref.as_ref()));
            fks.extend(fk_columns_from_where_ast(between_low_ast_box_ref.as_ref()));
            fks.extend(fk_columns_from_where_ast(between_high_ast_box_ref.as_ref()));
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
                fks.extend(fk_columns_from_where_ast(
                    case_else_results_ast_box.as_ref(),
                ));
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

#[derive(Debug)]
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

    /// The column of the table being referred by the foreign key
    pub foreign_key_column: String,

    /// Any child foreign key columns that are part of the original_ref string.
    pub nested_fks: Vec<ForeignKeyReference>,
}

impl ForeignKeyReference {
    /// Given a table name and list of table column names, return a list of foreign key references. If none of the provided columns are foreign keys, returns `Ok(None)`.
    ///
    /// # Examples
    ///
    /// ## Simple query (1 level deep)
    ///
    /// ```ignore
    /// // a_table.a_foreign_key references b_table.id
    /// // a_table.another_foreign_key references c_table.id
    ///
    /// assert_eq!(
    ///     ForeignKeyReference::from_query_columns(
    ///         client,
    ///         "a_table",
    ///         &[
    ///             "a_foreign_key.some_text",
    ///             "another_foreign_key.some_str",
    ///             "b"
    ///         ]
    ///     ),
    ///     Ok(Some(vec![
    ///         ForeignKeyReference {
    ///             original_refs: vec!["a_foreign_key.some_text".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "a_foreign_key".to_string(),
    ///             table_referred: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             nested_fks: vec![],
    ///         },
    ///         ForeignKeyReference {
    ///             original_refs: vec!["another_foreign_key.some_str".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "another_foreign_key".to_string(),
    ///             table_referred: "c_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             nested_fks: vec![],
    ///         }
    ///     ]))
    /// );
    /// ```
    ///
    /// ## Nested foreign keys
    ///
    /// ```ignore
    /// // a_foreign_key references b_table.id
    /// // another_foreign_key references c_table.id
    /// // another_foreign_key.nested_fk references d_table.id
    /// // another_foreign_key.different_nested_fk references e_table.id
    ///
    /// assert_eq!(
    ///     ForeignKeyReference::from_query_columns(
    ///         client,
    ///         "a_table",
    ///         &[
    ///             "a_foreign_key.some_text",
    ///             "another_foreign_key.nested_fk.some_str",
    ///             "another_foreign_key.different_nested_fk.some_int",
    ///             "b"
    ///         ]
    ///     ),
    ///     Ok(Some(vec![
    ///         ForeignKeyReference {
    ///             original_refs: vec!["a_foreign_key.some_text".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "a_foreign_key".to_string(),
    ///             table_referred: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             nested_fks: vec![]
    ///         },
    ///         ForeignKeyReference {
    ///             original_refs: vec!["another_foreign_key.nested_fk.some_str".to_string(), "another_foreign_key.different_nested_fk.some_int".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "another_foreign_key".to_string(),
    ///             table_referred: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             nested_fks: vec![
    ///                 ForeignKeyReference {
    ///                     original_refs: vec!["nested_fk.some_str".to_string()],
    ///                     referring_table: "c_table".to_string(),
    ///                     referring_column: "nested_fk".to_string(),
    ///                     table_referred: "d_table".to_string(),
    ///                     foreign_key_column: "id".to_string(),
    ///                     nested_fks: vec![]
    ///                 },
    ///                 ForeignKeyReference {
    ///                     original_refs: vec!["different_nested_fk.some_int".to_string()],
    ///                     referring_table: "c_table".to_string(),
    ///                     referring_column: "different_nested_fk".to_string(),
    ///                     table_referred: "e_table".to_string(),
    ///                     foreign_key_column: "id".to_string(),
    ///                     nested_fks: vec![]
    ///                 }
    ///             ]
    ///         }
    ///     ]))
    /// );
    /// ```
    pub fn from_query_columns(
        mut client: Client,
        table: &str,
        columns: &[&str],
    ) -> Result<(Vec<Self>, Client), (ApiError, Client)> {
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
            return Ok((vec![], client));
        }

        // group child columns & original column references by the parent column being referenced
        let mut fk_columns_grouped: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
        for col in fk_columns.into_iter() {
            if let Some(dot_index) = col.find('.') {
                if let (Some(parent_col_name), Some(child_column)) =
                    (col.get(0..dot_index), col.get(dot_index..))
                {
                    if !fk_columns_grouped.contains_key(parent_col_name) {
                        fk_columns_grouped.insert(
                            parent_col_name.to_string(),
                            (vec![child_column.to_string()], vec![col]),
                        );
                    } else {
                        let (child_columns, original_refs) =
                            fk_columns_grouped.get_mut(parent_col_name).unwrap();

                        child_columns.push(child_column.to_string());
                        original_refs.push(col.clone());
                    }
                }
            }
        }

        // get column stats for table

        // let fk_columns_grouped_borrow = RefCell::new(fk_columns_grouped);
        // let table_borrow = RefCell::new(table.to_string());

        let (stats, mut client) = select_column_stats_statement(&mut client, table)
            .then(move |result| match result {
                Ok(statement) => Ok((client.query(&statement, &[]), client)),
                Err(e) => Err((e, client)),
            })
            .map(|(query, client)| {
                select_column_stats(query).then(move |result| match result {
                    Ok(stats) => Ok((stats, client)),
                    Err(e) => Err((e, client)),
                })
            })
            .flatten()
            .map_err(|(e, client)| (ApiError::from(e), client))
            // TODO: [Kai@2019-06-15]: I'm not really happy that we have to block the async (otherwise rust throws E0720 at me), we should look at this again after async/await syntax changes come out
            .wait()?;

        // contains a (&str, &Vec<&str>, &Vec<&str>) tuple representing the matched parent column name, child columns, and original column strings
        let mut matched_columns = vec![];

        // filter the table column stats to just the foreign key columns that match the given columns
        let filtered_stats: Vec<TableColumnStat> = stats
            .into_iter()
            .filter(|stat| {
                if !stat.is_foreign_key {
                    return false;
                }

                // let fk_columns_grouped_borrow_instance = fk_columns_grouped_borrow.borrow();
                match fk_columns_grouped
                    .iter()
                    .find(|(parent_col, _child_col_vec)| *parent_col == &stat.column_name)
                {
                    Some((
                        matched_parent_fk_column,
                        (matched_child_col_vec, matched_orig_refs),
                    )) => {
                        matched_columns.push((
                            matched_parent_fk_column,
                            matched_child_col_vec,
                            matched_orig_refs,
                        ));
                        true
                    }
                    None => false,
                }
            })
            .collect();

        // stats and matched_columns should have the same length and their indexes should match

        // Map the Vec of `TableColumnStat`s to a Vec of `ForeignKeyReference`
        let mut fkrs = vec![];
        for (i, stat) in filtered_stats.iter().enumerate() {
            let (_parent_col_match, child_columns_match, original_refs_match) = &matched_columns[i];

            let original_refs = original_refs_match
                .iter()
                .map(|col| col.to_string())
                .collect();
            let foreign_key_table = if let Some(t) = &stat.foreign_key_table {
                t.clone()
            } else {
                "".to_string()
            };

            // filter child columns to just the foreign keys
            let child_fk_columns: Vec<&str> = child_columns_match
                .iter()
                .filter_map(|child_col| {
                    if !child_col.contains('.') {
                        return None;
                    }

                    let first_dot_pos = child_col.find('.').unwrap();
                    Some(&child_col[first_dot_pos + 1..])
                })
                .collect();

            // child column is not a foreign key, return future with ForeignKeyReference
            if child_fk_columns.is_empty() {
                fkrs.push(ForeignKeyReference {
                    referring_column: stat.column_name.clone(),
                    referring_table: table.to_string(),
                    table_referred: foreign_key_table,
                    foreign_key_column: if let Some(t) = &stat.foreign_key_column {
                        t.clone()
                    } else {
                        "".to_string()
                    },
                    nested_fks: vec![],
                    original_refs,
                });
            } else {
                // child columns are all FKs, so we need to recursively call this function
                // TODO: [Kai@2019-06-15]: I'm not really happy that we have to block the async (otherwise rust throws E0720 at me), we should look at this again after async/await syntax changes come out
                let foreign_key_column = match &stat.foreign_key_column {
                    Some(column) => column.to_string(),
                    None => "".to_string(),
                };

                let (nested_fks, client_reuse) =
                    Self::from_query_columns(client, &foreign_key_table, &child_fk_columns)?;
                client = client_reuse;

                fkrs.push(ForeignKeyReference {
                    referring_column: stat.column_name.clone(),
                    referring_table: table.to_string(),
                    table_referred: foreign_key_table,
                    foreign_key_column,
                    nested_fks,
                    original_refs,
                })
            }
        }

        Ok((fkrs, client))
    }

    /// Given an array of ForeignKeyReference, find the one that matches a given table and column name, as well as the matching foreign key table column.
    pub fn find<'a>(refs: &'a [Self], table: &str, col: &'a str) -> Option<(&'a Self, &'a str)> {
        for fkr in refs {
            if fkr.referring_table != table {
                continue;
            }

            // see if any of the `ForeignKeyReference`s have any original references that match the given column name
            let found_orig_ref = fkr.original_refs.iter().find(|ref_col| col == *ref_col);

            if found_orig_ref.is_some() {
                if !&fkr.nested_fks.is_empty() {
                    let first_dot_pos = col.find('.').unwrap();
                    let sub_column_str = &col[first_dot_pos + 1..];
                    return Self::find(&fkr.nested_fks, &fkr.table_referred, sub_column_str);
                }

                // by this point, sub_column_breadcrumbs should have a length of 1 or 2 (1 if there's no FK, 2 if there is a FK). If the original column string had more than 1 level for foreign keys, the function would have recursively called itself until it got to this point
                let sub_column_breadcrumbs: Vec<&str> = col.split('.').collect();
                let sub_column_str = if sub_column_breadcrumbs.len() > 1 {
                    sub_column_breadcrumbs[1]
                } else {
                    col
                };
                return Some((fkr, sub_column_str));
            }
        }

        None
    }

    /// Given a list of foreign key references, construct the `INNER JOIN` SQL string to be used in a query.
    pub fn inner_join_expr(fk_refs: &[Self]) -> String {
        // a vec of tuples where each tuple contains: referring table name, referring table column to equate, fk table name to join with, fk table column to equate
        let join_data = Self::inner_join_expr_calc(fk_refs);

        join_data
            .iter()
            .map(
                |(referring_table, referring_column, referred_table, referred_column)| {
                    // generate the INNER JOIN column equality expression
                    [
                        referred_table,
                        " ON ",
                        referring_table,
                        ".",
                        referring_column,
                        " = ",
                        referred_table,
                        ".",
                        referred_column,
                    ]
                    .join("")
                },
            )
            .collect::<Vec<String>>()
            .join("\nINNER JOIN ")
    }

    fn inner_join_expr_calc(fk_refs: &[Self]) -> Vec<(&str, &str, &str, &str)> {
        // a vec of tuples where each tuple contains: referring table name, referring table column to equate, fk table name to join with, fk table column to equate
        let mut join_data: Vec<(&str, &str, &str, &str)> = vec![];

        for fk in fk_refs {
            join_data.push((
                &fk.referring_table,
                &fk.referring_column,
                &fk.table_referred,
                &fk.foreign_key_column,
            ));

            join_data.extend(Self::inner_join_expr_calc(&fk.nested_fks));
        }

        join_data
    }
}

#[cfg(test)]
mod where_clause_str_to_ast_tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use sqlparser::sqlast::SQLOperator;

    #[test]
    fn basic() {
        let clause = "a > b";
        let expected = ASTNode::SQLBinaryExpr {
            left: Box::new(ASTNode::SQLIdentifier("a".to_string())),
            op: SQLOperator::Gt,
            right: Box::new(ASTNode::SQLIdentifier("b".to_string())),
        };
        assert_eq!(where_clause_str_to_ast(clause).unwrap().unwrap(), expected);
    }

    #[test]
    fn foreign_keys() {
        let clause = "a.b > c";
        let expected = ASTNode::SQLBinaryExpr {
            left: Box::new(ASTNode::SQLCompoundIdentifier(vec![
                "a".to_string(),
                "b".to_string(),
            ])),
            op: SQLOperator::Gt,
            right: Box::new(ASTNode::SQLIdentifier("c".to_string())),
        };
        assert_eq!(where_clause_str_to_ast(clause).unwrap().unwrap(), expected);
    }

    #[test]
    fn empty_string_returns_error() {
        let clause = "";
        assert!(where_clause_str_to_ast(clause).is_err());
    }

    #[test]
    fn empty_parentheses_returns_err() {
        let clause = "()";
        assert!(where_clause_str_to_ast(clause).is_err());
    }

    #[test]
    fn invalid_clause_returns_err() {
        let clause = "not valid WHERE syntax";
        assert!(where_clause_str_to_ast(clause).is_err());
    }
}

#[cfg(test)]
mod fk_ast_nodes_from_where_ast {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    #[test]
    fn basic() {
        let ast =
            ASTNode::SQLCompoundIdentifier(vec!["a_column".to_string(), "b_column".to_string()]);

        let mut borrowed = Rc::new(ast);
        let mut cloned = borrowed.clone();

        let expected = vec![("a_column.b_column".to_string(), Rc::make_mut(&mut borrowed))];

        assert_eq!(
            fk_ast_nodes_from_where_ast(Rc::make_mut(&mut cloned)),
            expected
        );
    }

    #[test]
    fn non_fk_nodes_return_empty_vec() {
        let mut ast = ASTNode::SQLIdentifier("a_column".to_string());
        let expected = vec![];

        assert_eq!(fk_ast_nodes_from_where_ast(&mut ast), expected);
    }
}

#[cfg(test)]
mod fk_columns_from_where_ast {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic() {
        let ast =
            ASTNode::SQLCompoundIdentifier(vec!["a_column".to_string(), "b_column".to_string()]);

        let expected = vec!["a_column.b_column".to_string()];

        assert_eq!(fk_columns_from_where_ast(&ast), expected);
    }

    #[test]
    fn non_fk_nodes_return_empty_vec() {
        let mut ast = ASTNode::SQLIdentifier("a_column".to_string());
        let expected = vec![];

        assert_eq!(fk_ast_nodes_from_where_ast(&mut ast), expected);
    }
}

#[cfg(test)]
mod fkr_find {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic() {
        let refs = vec![ForeignKeyReference {
            original_refs: vec!["a_foreign_key.some_text".to_string()],
            referring_table: "a_table".to_string(),
            referring_column: "a_foreign_key".to_string(),
            table_referred: "b_table".to_string(),
            foreign_key_column: "id".to_string(),
            nested_fks: vec![],
        }];

        let (found_fk, fk_table_column) =
            ForeignKeyReference::find(&refs, "a_table", "a_foreign_key.some_text").unwrap();

        assert_eq!(found_fk as *const _, &refs[0] as *const _); // comparing pointers to see if they both point at the same item
        assert_eq!(fk_table_column, "some_text");
    }

    #[test]
    fn nested_foreign_keys() {
        let refs = vec![ForeignKeyReference {
            original_refs: vec!["another_foreign_key.nested_fk.some_str".to_string()],
            referring_table: "a_table".to_string(),
            referring_column: "another_foreign_key".to_string(),
            table_referred: "b_table".to_string(),
            foreign_key_column: "id".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["nested_fk.some_str".to_string()],
                referring_table: "b_table".to_string(),
                referring_column: "nested_fk".to_string(),
                table_referred: "c_table".to_string(),
                foreign_key_column: "id".to_string(),
                nested_fks: vec![],
            }],
        }];

        let (found_fk, fk_table_column) =
            ForeignKeyReference::find(&refs, "a_table", "another_foreign_key.nested_fk.some_str")
                .unwrap();

        let nested_fk = &refs[0].nested_fks[0];

        assert_eq!(found_fk as *const _, nested_fk as *const _); // comparing pointers to see if they both point at the same item
        assert_eq!(fk_table_column, "some_str");
    }
}

#[cfg(test)]
mod fkr_inner_join_expr {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn nested_foreign_keys() {
        let refs = vec![
            ForeignKeyReference {
                original_refs: vec!["another_foreign_key.nested_fk.some_str".to_string()],
                referring_table: "a_table".to_string(),
                referring_column: "another_foreign_key".to_string(),
                table_referred: "b_table".to_string(),
                foreign_key_column: "id".to_string(),
                nested_fks: vec![ForeignKeyReference {
                    original_refs: vec!["nested_fk.some_str".to_string()],
                    referring_table: "b_table".to_string(),
                    referring_column: "nested_fk".to_string(),
                    table_referred: "d_table".to_string(),
                    foreign_key_column: "id".to_string(),
                    nested_fks: vec![],
                }],
            },
            ForeignKeyReference {
                original_refs: vec!["fk.another_field".to_string()],
                referring_table: "b_table".to_string(),
                referring_column: "b_table_fk".to_string(),
                table_referred: "e_table".to_string(),
                foreign_key_column: "id".to_string(),
                nested_fks: vec![],
            },
        ];

        assert_eq!(ForeignKeyReference::inner_join_expr(&refs), "b_table ON a_table.another_foreign_key = b_table.id\nINNER JOIN d_table ON b_table.nested_fk = d_table.id\nINNER JOIN e_table ON b_table.b_table_fk = e_table.id".to_string());
    }
}
