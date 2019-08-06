use actix::Addr;
use futures::future::{err, join_all, ok, Either, Future};
use rayon::prelude::*;
use sqlparser::ast::{Expr, Function};
use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    sync::Arc,
};

use super::select_table_stats::{
    select_column_stats, select_column_stats_statement, TableColumnStat,
};
use crate::{
    db::connect,
    stats_cache::{StatsCache, StatsCacheMessage, StatsCacheResponse},
    Error,
};

/// Extracts the "a_table".* (wildcard), identifiers, and foreign key Exprs from a WHERE Expr. The
/// Expressions are returned as a Vector of tuples: (The expr-as-a-string, a reference to the
/// expression itself).
pub fn fk_ast_nodes_from_where_ast(ast: &mut Expr) -> Vec<(String, &mut Expr)> {
    let mut fks = vec![];

    match ast {
        Expr::QualifiedWildcard(wildcard_vec) => {
            fks.push((wildcard_vec.join("."), ast));
        }
        Expr::CompoundIdentifier(nested_fk_column_vec) => {
            fks.push((nested_fk_column_vec.join("."), ast));
        }
        Expr::Identifier(non_nested_column_name) => {
            fks.push((non_nested_column_name.clone(), ast));
        }
        Expr::IsNull(null_ast_box) => {
            fks.par_extend(fk_ast_nodes_from_where_ast(null_ast_box.borrow_mut()));
        }
        Expr::IsNotNull(null_ast_box) => {
            fks.par_extend(fk_ast_nodes_from_where_ast(null_ast_box.borrow_mut()));
        }
        Expr::InList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(
                list_expr_ast_box_ref.borrow_mut(),
            ));

            for list_ast in list_ast_vec {
                fks.par_extend(fk_ast_nodes_from_where_ast(list_ast));
            }
        }
        Expr::BinaryOp {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(
                bin_left_ast_box_ref.borrow_mut(),
            ));
            fks.par_extend(fk_ast_nodes_from_where_ast(
                bin_right_ast_box_ref.borrow_mut(),
            ));
        }
        Expr::Cast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(cast_expr_box_ref.borrow_mut()));
        }
        Expr::Nested(nested_ast_box_ref) => {
            fks.par_extend(fk_ast_nodes_from_where_ast(nested_ast_box_ref.borrow_mut()));
        }
        Expr::UnaryOp {
            expr: unary_expr_box_ref,
            ..
        } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(unary_expr_box_ref.borrow_mut()));
        }
        Expr::Between {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(
                between_expr_ast_box_ref.borrow_mut(),
            ));
            fks.par_extend(fk_ast_nodes_from_where_ast(
                between_low_ast_box_ref.borrow_mut(),
            ));
            fks.par_extend(fk_ast_nodes_from_where_ast(
                between_high_ast_box_ref.borrow_mut(),
            ));
        }
        Expr::Function(Function {
            args: args_ast_vec, ..
        }) => {
            for ast_arg in args_ast_vec {
                fks.par_extend(fk_ast_nodes_from_where_ast(ast_arg));
            }
        }
        Expr::Case {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
            ..
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                fks.par_extend(fk_ast_nodes_from_where_ast(case_condition_ast));
            }

            for case_results_ast_vec in case_results_ast_vec {
                fks.par_extend(fk_ast_nodes_from_where_ast(case_results_ast_vec));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                fks.par_extend(fk_ast_nodes_from_where_ast(
                    case_else_results_ast_box.borrow_mut(),
                ));
            }
        }
        Expr::Collate { expr, .. } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(expr.borrow_mut()))
        }
        Expr::Extract { expr, .. } => {
            fks.par_extend(fk_ast_nodes_from_where_ast(expr.borrow_mut()))
        }
        // below is unsupported
        Expr::Exists(_query_box) => (), // EXISTS(subquery) not supported
        Expr::Wildcard => (),
        Expr::InSubquery { .. } => (), // subqueries in WHERE statement are not supported
        Expr::Value(_val) => (),
        Expr::Subquery(_query_box) => (), // subqueries in WHERE are not supported
    };

    fks
}

/// Similar to `fk_ast_nodes_from_where_ast`. Extracts the raw/incorrect foreign key column strings
/// from a WHERE Expr
pub fn fk_columns_from_where_ast(ast: &Expr) -> Vec<String> {
    let mut fks = vec![];

    match ast {
        Expr::QualifiedWildcard(wildcard_vec) => {
            fks.push(wildcard_vec.join("."));
        }
        Expr::CompoundIdentifier(nested_fk_column_vec) => {
            fks.push(nested_fk_column_vec.join("."));
        }
        Expr::IsNull(null_ast_box) => {
            fks.par_extend(fk_columns_from_where_ast(null_ast_box.as_ref()));
        }
        Expr::IsNotNull(null_ast_box) => {
            fks.par_extend(fk_columns_from_where_ast(null_ast_box.as_ref()));
        }
        Expr::InList {
            expr: list_expr_ast_box_ref,
            list: list_ast_vec,
            ..
        } => {
            fks.par_extend(fk_columns_from_where_ast(list_expr_ast_box_ref.as_ref()));

            for list_ast in list_ast_vec {
                fks.par_extend(fk_columns_from_where_ast(list_ast));
            }
        }
        Expr::BinaryOp {
            left: bin_left_ast_box_ref,
            right: bin_right_ast_box_ref,
            ..
        } => {
            fks.par_extend(fk_columns_from_where_ast(bin_left_ast_box_ref.as_ref()));
            fks.par_extend(fk_columns_from_where_ast(bin_right_ast_box_ref.as_ref()));
        }
        Expr::Cast {
            expr: cast_expr_box_ref,
            ..
        } => {
            fks.par_extend(fk_columns_from_where_ast(cast_expr_box_ref.as_ref()));
        }
        Expr::Nested(nested_ast_box_ref) => {
            fks.par_extend(fk_columns_from_where_ast(nested_ast_box_ref.as_ref()));
        }
        Expr::UnaryOp {
            expr: unary_expr_box_ref,
            ..
        } => {
            fks.par_extend(fk_columns_from_where_ast(unary_expr_box_ref.as_ref()));
        }
        Expr::Between {
            expr: between_expr_ast_box_ref,
            low: between_low_ast_box_ref,
            high: between_high_ast_box_ref,
            ..
        } => {
            fks.par_extend(fk_columns_from_where_ast(between_expr_ast_box_ref.as_ref()));
            fks.par_extend(fk_columns_from_where_ast(between_low_ast_box_ref.as_ref()));
            fks.par_extend(fk_columns_from_where_ast(between_high_ast_box_ref.as_ref()));
        }
        Expr::Function(Function {
            args: args_ast_vec, ..
        }) => {
            for ast_arg in args_ast_vec {
                fks.par_extend(fk_columns_from_where_ast(ast_arg));
            }
        }
        Expr::Case {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
            ..
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                fks.par_extend(fk_columns_from_where_ast(case_condition_ast));
            }

            for case_results_ast_vec in case_results_ast_vec {
                fks.par_extend(fk_columns_from_where_ast(case_results_ast_vec));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                fks.par_extend(fk_columns_from_where_ast(
                    case_else_results_ast_box.as_ref(),
                ));
            }
        }
        Expr::Collate { expr, .. } => fks.par_extend(fk_columns_from_where_ast(expr.as_ref())),
        Expr::Extract { expr, .. } => fks.par_extend(fk_columns_from_where_ast(expr.as_ref())),
        // below is unsupported
        Expr::Exists(_query_box) => (), // EXISTS(subquery) not supported
        Expr::Identifier(_non_nested_column_name) => (),
        Expr::Wildcard => (),
        Expr::InSubquery { .. } => (), // subqueries in WHERE statement are not supported
        Expr::Value(_val) => (),
        Expr::Subquery(_query_box) => (), // subqueries in WHERE are not supported
    };

    fks
}

type ChildColumns = Vec<String>;
type OriginalColumnReferences = Vec<String>;

#[derive(Debug)]
/// Represents a single foreign key, usually generated by a queried column using dot-syntax.
pub struct ForeignKeyReference {
    /// The original column strings referencing a (possibly nested) foreign key value.
    pub original_refs: OriginalColumnReferences,

    /// The parent table name that contains the foreign key column.
    pub referring_table: String,

    /// The parent table’s column name that is the foreign key.
    pub referring_column: String,

    // The Postgres type of the referring column.
    pub referring_column_type: &'static str,

    /// The table being referred by the foreign key.
    pub foreign_key_table: String,

    /// A Vec of table stats for the table being referred by the foreign key.
    pub foreign_key_table_stats: Vec<TableColumnStat>,

    /// The column of the table being referred by the foreign key.
    pub foreign_key_column: String,

    // The Postgres type of the column of the table being referred by the foreign key.
    pub foreign_key_column_type: &'static str,

    /// Any child foreign key columns that are part of the original_ref string.
    pub nested_fks: Vec<ForeignKeyReference>,
}

impl ForeignKeyReference {
    /// Given a table name and list of table column names, return a list of foreign key references.
    /// If none of the provided columns are foreign keys, returns `Ok(None)`.
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
    ///             foreign_key_table: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             foreign_key_column_type: "id".to_string(),
    ///             nested_fks: vec![],
    ///         },
    ///         ForeignKeyReference {
    ///             original_refs: vec!["another_foreign_key.some_str".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "another_foreign_key".to_string(),
    ///             foreign_key_table: "c_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             foreign_key_column_type: "id".to_string(),
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
    ///             foreign_key_table: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             foreign_key_column_type: "id".to_string(),
    ///             nested_fks: vec![]
    ///         },
    ///         ForeignKeyReference {
    ///             original_refs: vec!["another_foreign_key.nested_fk.some_str".to_string(), "another_foreign_key.different_nested_fk.some_int".to_string()],
    ///             referring_table: "a_table".to_string(),
    ///             referring_column: "another_foreign_key".to_string(),
    ///             foreign_key_table: "b_table".to_string(),
    ///             foreign_key_column: "id".to_string(),
    ///             foreign_key_column_type: "id".to_string(),
    ///             nested_fks: vec![
    ///                 ForeignKeyReference {
    ///                     original_refs: vec!["nested_fk.some_str".to_string()],
    ///                     referring_table: "c_table".to_string(),
    ///                     referring_column: "nested_fk".to_string(),
    ///                     foreign_key_table: "d_table".to_string(),
    ///                     foreign_key_column: "id".to_string(),
    ///                     foreign_key_column_type: "id".to_string(),
    ///                     nested_fks: vec![]
    ///                 },
    ///                 ForeignKeyReference {
    ///                     original_refs: vec!["different_nested_fk.some_int".to_string()],
    ///                     referring_table: "c_table".to_string(),
    ///                     referring_column: "different_nested_fk".to_string(),
    ///                     foreign_key_table: "e_table".to_string(),
    ///                     foreign_key_column: "id".to_string(),
    ///                     foreign_key_column_type: "id".to_string(),
    ///                     nested_fks: vec![]
    ///                 }
    ///             ]
    ///         }
    ///     ]))
    /// );
    /// ```
    pub(crate) fn from_query_columns(
        db_url: &str,
        stats_cache_addr: Arc<Option<Addr<StatsCache>>>,
        table: String,
        columns: Vec<String>,
    ) -> Box<dyn Future<Item = Vec<Self>, Error = Error> + Send> {
        let mut fk_columns: Vec<String> = columns
            .par_iter()
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
            let empty_future = ok(vec![]);
            return Box::new(empty_future);
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
                        original_refs.push(col);
                    }
                }
            }
        }

        // get column stats for table
        let process_column_stats_future = ForeignKeyReference::process_column_stats(
            db_url,
            stats_cache_addr,
            table,
            fk_columns_grouped,
        );

        Box::new(process_column_stats_future)
    }

    fn get_table_column_stats(
        db_url_str: &str,
        table: &str,
    ) -> impl Future<Item = Vec<TableColumnStat>, Error = Error> {
        let table_clone = table.to_string();
        connect(db_url_str)
            .map_err(Error::from)
            .and_then(move |mut conn| {
                select_column_stats_statement(&mut conn, &table_clone)
                    .map_err(Error::from)
                    .and_then(move |statement| {
                        let q = conn.query(&statement, &[]);
                        select_column_stats(q).map_err(Error::from)
                    })
            })
    }

    fn process_column_stats(
        db_url: &str,
        stats_cache_addr: Arc<Option<Addr<StatsCache>>>,
        table: String,
        fk_columns_grouped: HashMap<String, (Vec<String>, Vec<String>)>,
    ) -> impl Future<Item = Vec<Self>, Error = Error> + Send {
        // used later in futures
        let table_clone = table.clone();
        let table_clone_2 = table.clone();
        let db_url_str = db_url.to_string();
        let db_url_str_2 = db_url_str.clone();

        let column_stats_future = if let Some(cache_addr) = stats_cache_addr.borrow() {
            // similar to select_table_stats::select_table_stats_from_cache, except we just need a
            // subset of stats (cheaper/more lightweight to query), not the whole stat object
            // (expensive to query)
            let cache_future = cache_addr
                .send(StatsCacheMessage::FetchStatsForTable(table))
                .map_err(Error::from)
                .and_then(move |response_result| match response_result {
                    Ok(response) => match response {
                        StatsCacheResponse::TableStat(stats_opt) => match stats_opt {
                            Some(stats) => {
                                let calculated_columns_and_stats =
                                    Self::match_fk_column_stats(stats.columns, fk_columns_grouped);
                                Either::A(ok(calculated_columns_and_stats))
                            }
                            None => Either::B(
                                Self::get_table_column_stats(&db_url_str, &table_clone).map(
                                    move |stats| {
                                        Self::match_fk_column_stats(stats, fk_columns_grouped)
                                    },
                                ),
                            ),
                        },
                        StatsCacheResponse::OK => unreachable!(
                            "Message of type `FetchStatsForTable` should never return an OK."
                        ),
                    },
                    Err(e) => Either::A(err(e)),
                });
            Either::A(cache_future)
        } else {
            Either::B(
                Self::get_table_column_stats(&db_url_str, &table_clone)
                    .map(move |stats| Self::match_fk_column_stats(stats, fk_columns_grouped)),
            )
        };

        column_stats_future
            // stats and matched_columns should have the same length and their indexes should match
            .and_then(move |(filtered_stats, matched_columns)| {
                ForeignKeyReference::stats_to_fkr_futures(
                    &db_url_str_2,
                    stats_cache_addr,
                    table_clone_2,
                    filtered_stats,
                    matched_columns,
                )
                .map_err(Error::from)
            })
    }

    /// Uses a given hashmap of parent column:(child/fk columns, original column references) and
    /// TableColumnStats, calculate the matching column stats that are foreign keys as well as a
    /// vector of tuples representing column matches: (parent FK column, child columns, original
    /// references)
    fn match_fk_column_stats(
        stats: Vec<TableColumnStat>,
        fk_columns_grouped: HashMap<String, (ChildColumns, OriginalColumnReferences)>,
    ) -> (
        Vec<TableColumnStat>,
        Vec<(String, ChildColumns, OriginalColumnReferences)>,
    ) {
        // contains a tuple representing the (matched parent column name, child columns, and
        // original column strings)
        let mut matched_columns: Vec<(String, Vec<String>, Vec<String>)> = vec![];

        // filter the table column stats to just the foreign key columns that match the
        // given columns
        let filtered_stats: Vec<TableColumnStat> = stats
            .into_iter()
            .filter(|stat| {
                if !stat.is_foreign_key {
                    return false;
                }

                match fk_columns_grouped
                    .par_iter()
                    .find_any(|(parent_col, _child_col_vec)| parent_col == &&stat.column_name)
                {
                    Some((
                        matched_parent_fk_column,
                        (matched_child_col_vec, matched_orig_refs),
                    )) => {
                        matched_columns.push((
                            matched_parent_fk_column.to_string(),
                            matched_child_col_vec
                                .par_iter()
                                .map(|s| s.to_string())
                                .collect(),
                            matched_orig_refs.clone(),
                        ));
                        true
                    }
                    None => false,
                }
            })
            .collect();

        (filtered_stats, matched_columns)
    }

    /// Maps a Vec of `TableColumnStat`s to a Future resolving to a Vec of `ForeignKeyReference`s.
    /// Used by from_query_columns.
    fn stats_to_fkr_futures(
        db_url: &str,
        stats_cache_addr: Arc<Option<Addr<StatsCache>>>,
        table: String,
        stats: Vec<TableColumnStat>,
        matched_columns: Vec<(String, Vec<String>, Vec<String>)>,
    ) -> impl Future<Item = Vec<Self>, Error = Error> {
        let mut fkr_futures = vec![];

        for (i, stat) in stats.into_iter().enumerate() {
            // stats.into_iter().enumerate().map(move |(i, stat)| {
            let (_parent_col_match, child_columns_match, original_refs_match) = &matched_columns[i];

            let original_refs = original_refs_match
                .par_iter()
                .map(|col| col.to_string())
                .collect();
            let foreign_key_table = stat.foreign_key_table.clone().unwrap();

            // filter child columns to just the foreign keys
            let child_fk_columns: Vec<String> = child_columns_match
                .par_iter()
                .filter_map(|child_col| {
                    if !child_col.contains('.') {
                        return None;
                    }

                    let first_dot_pos = child_col.find('.').unwrap();
                    Some(child_col[first_dot_pos + 1..].to_string())
                })
                .collect();

            let table_clone = table.clone();
            let stat_column_name_clone = stat.column_name.clone();
            let stat_fk_column = stat.foreign_key_column.clone().unwrap_or_else(String::new);
            let stat_fk_column_type = stat.foreign_key_column_type.unwrap_or_else(|| "");

            let fk_column_stats_future = if let Some(cache_addr) = stats_cache_addr.borrow() {
                // used for moving into futures
                let db_url_clone = db_url.to_string();
                let fk_table_clone = foreign_key_table.clone();

                // similar to select_table_stats::select_table_stats_from_cache, except we just
                // need a subset of stats (cheaper/more lightweight to
                // query), not the whole stat object (expensive to query)
                let cache_future = cache_addr
                    .send(StatsCacheMessage::FetchStatsForTable(
                        foreign_key_table.clone(),
                    ))
                    .map_err(Error::from)
                    .and_then(move |response_result| match response_result {
                        Ok(response) => match response {
                            StatsCacheResponse::TableStat(stats_opt) => match stats_opt {
                                Some(stats) => Either::A(ok(stats.columns)),
                                None => Either::B(Self::get_table_column_stats(
                                    &db_url_clone,
                                    &fk_table_clone.clone(),
                                )),
                            },
                            StatsCacheResponse::OK => unreachable!(
                                "Message of type `FetchStatsForTable` should never return an OK."
                            ),
                        },
                        Err(e) => Either::A(err(e)),
                    });
                Either::A(cache_future)
            } else {
                Either::B(Self::get_table_column_stats(
                    &db_url,
                    &foreign_key_table.clone(),
                ))
            };

            // child column is not a foreign key, return future with ForeignKeyReference
            if child_fk_columns.is_empty() {
                let resolve_stats_to_fkr =
                    fk_column_stats_future.map(move |stats| ForeignKeyReference {
                        referring_column: stat_column_name_clone,
                        referring_column_type: stat.column_type,
                        referring_table: table_clone,
                        foreign_key_table,
                        foreign_key_table_stats: stats,
                        foreign_key_column: stat_fk_column,
                        foreign_key_column_type: stat_fk_column_type,
                        nested_fks: vec![],
                        original_refs,
                    });

                fkr_futures.push(Either::A(resolve_stats_to_fkr));
                continue;
            }

            // child columns are all FKs, so we need to recursively call this function
            let child_columns_future = Self::from_query_columns(
                db_url,
                Arc::clone(&stats_cache_addr),
                foreign_key_table.clone(),
                child_fk_columns,
            )
            .join(fk_column_stats_future)
            .and_then(move |(nested_fks, foreign_key_table_stats)| {
                Ok(ForeignKeyReference {
                    referring_column: stat_column_name_clone,
                    referring_column_type: stat.column_type,
                    referring_table: table_clone,
                    foreign_key_table,
                    foreign_key_table_stats,
                    foreign_key_column: stat_fk_column,
                    foreign_key_column_type: stat_fk_column_type,
                    nested_fks,
                    original_refs,
                })
            });

            fkr_futures.push(Either::B(child_columns_future));
        }

        join_all(fkr_futures)
    }

    /// Given an array of ForeignKeyReference, find the one that matches a given table and column
    /// name, as well as the matching foreign key table column.
    pub fn find<'a>(refs: &'a [Self], table: &str, col: &'a str) -> Option<(&'a Self, &'a str)> {
        for fkr in refs {
            if fkr.referring_table != table {
                continue;
            }

            // see if any of the `ForeignKeyReference`s have any original references that match the
            // given column name
            let found_orig_ref = fkr
                .original_refs
                .par_iter()
                .find_any(|ref_col| col == *ref_col);

            if found_orig_ref.is_some() {
                if !fkr.nested_fks.is_empty() {
                    let first_dot_pos = col.find('.').unwrap();
                    let sub_column_str = &col[first_dot_pos + 1..];

                    if !sub_column_str.contains('.') {
                        return Some((fkr, sub_column_str));
                    }

                    return Self::find(&fkr.nested_fks, &fkr.foreign_key_table, sub_column_str);
                }

                // by this point, sub_column_breadcrumbs should have a length of 1 or 2 (1 if
                // there's no FK, 2 if there is a FK). If the original column string had more than 1
                // level for foreign keys, the function would have recursively called itself until
                // it got to this point
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

    /// Given a list of foreign key references, construct a SQL string to be used in a query (INNER
    /// JOIN, for example). Accepts 1) a closure that operates on a tuple argument: (referring
    /// table, referring column, table referred, foreign key column referred), and returns a String,
    /// and 2) a string that is used to join the strings emitted the function.
    pub fn join_foreign_key_references<F>(
        fk_refs: &[Self],
        str_conversion_fn: F,
        join_str: &str,
    ) -> String
    where
        F: FnMut((&str, &str, &str, &str)) -> String,
    {
        // a vec of tuples where each tuple contains: referring table name, referring table column
        // to equate, fk table name to join with, fk table column to equate
        let join_data = Self::fk_join_expr_calc(fk_refs);

        join_data
            .into_iter()
            .map(str_conversion_fn)
            .collect::<Vec<String>>()
            .join(join_str)
    }

    fn fk_join_expr_calc(fk_refs: &[Self]) -> Vec<(&str, &str, &str, &str)> {
        // a vec of tuples where each tuple contains: referring table name, referring table column
        // to equate, fk table name to join with, fk table column to equate
        let mut join_data: Vec<(&str, &str, &str, &str)> = vec![];

        for fk in fk_refs {
            join_data.push((
                &fk.referring_table,
                &fk.referring_column,
                &fk.foreign_key_table,
                &fk.foreign_key_column,
            ));

            join_data.par_extend(Self::fk_join_expr_calc(&fk.nested_fks));
        }

        join_data
    }
}

#[cfg(test)]
mod fk_ast_nodes_from_where_ast {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::rc::Rc;

    #[test]
    fn basic() {
        let ast = Expr::CompoundIdentifier(vec!["a_column".to_string(), "b_column".to_string()]);

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
        let ast = Expr::Identifier("a_column".to_string());

        let mut borrowed = Rc::new(ast);
        let mut cloned = borrowed.clone();

        let expected = vec![("a_column".to_string(), Rc::make_mut(&mut borrowed))];

        assert_eq!(
            fk_ast_nodes_from_where_ast(Rc::make_mut(&mut cloned)),
            expected
        );
    }
}

#[cfg(test)]
mod fk_columns_from_where_ast {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic() {
        let ast = Expr::CompoundIdentifier(vec!["a_column".to_string(), "b_column".to_string()]);

        let expected = vec!["a_column.b_column".to_string()];

        assert_eq!(fk_columns_from_where_ast(&ast), expected);
    }

    #[test]
    fn non_fk_nodes_return_empty_vec() {
        let ast = Expr::Identifier("a_column".to_string());
        let expected: Vec<String> = vec![];

        assert_eq!(fk_columns_from_where_ast(&ast), expected);
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
            referring_column_type: "int8",
            foreign_key_table: "b_table".to_string(),
            foreign_key_table_stats: vec![],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
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
            referring_column_type: "int8",
            foreign_key_table: "b_table".to_string(),
            foreign_key_table_stats: vec![],
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8",
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["nested_fk.some_str".to_string()],
                referring_table: "b_table".to_string(),
                referring_column: "nested_fk".to_string(),
                referring_column_type: "int8",
                foreign_key_table: "c_table".to_string(),
                foreign_key_table_stats: vec![],
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8",
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
mod fkr_join_foreign_key_references {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn nested_foreign_keys() {
        let refs = vec![
            ForeignKeyReference {
                original_refs: vec!["another_foreign_key.nested_fk.some_str".to_string()],
                referring_table: "a_table".to_string(),
                referring_column: "another_foreign_key".to_string(),
                referring_column_type: "int8",
                foreign_key_table: "b_table".to_string(),
                foreign_key_table_stats: vec![],
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8",
                nested_fks: vec![ForeignKeyReference {
                    original_refs: vec!["nested_fk.some_str".to_string()],
                    referring_table: "b_table".to_string(),
                    referring_column: "nested_fk".to_string(),
                    referring_column_type: "int8",
                    foreign_key_table: "d_table".to_string(),
                    foreign_key_table_stats: vec![],
                    foreign_key_column: "id".to_string(),
                    foreign_key_column_type: "int8",
                    nested_fks: vec![],
                }],
            },
            ForeignKeyReference {
                original_refs: vec!["fk.another_field".to_string()],
                referring_table: "b_table".to_string(),
                referring_column: "b_table_fk".to_string(),
                referring_column_type: "int8",
                foreign_key_table: "e_table".to_string(),
                foreign_key_table_stats: vec![],
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8",
                nested_fks: vec![],
            },
        ];

        assert_eq!(ForeignKeyReference::join_foreign_key_references(&refs, |(referring_table, referring_column, referred_table, referred_column)| {
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
        }, "\nINNER JOIN "), "b_table ON a_table.another_foreign_key = b_table.id\nINNER JOIN d_table ON b_table.nested_fk = d_table.id\nINNER JOIN e_table ON b_table.b_table_fk = e_table.id".to_string());
    }
}
