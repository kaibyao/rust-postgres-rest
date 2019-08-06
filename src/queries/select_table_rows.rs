use futures::{
    future::{err, Either, Future},
    stream::Stream,
};
use lazy_static::lazy_static;
use rayon::prelude::*;
use regex::Regex;
use sqlparser::ast::Expr;
use std::sync::Arc;
use tokio_postgres::types::ToSql;

use super::{
    foreign_keys::{fk_columns_from_where_ast, ForeignKeyReference},
    postgres_types::{row_to_row_values, RowValues, TypedColumnValue},
    query_types::QueryParamsSelect,
    select_table_stats::{select_column_stats, select_column_stats_statement, TableColumnStat},
    utils::{
        conditions_params_to_ast, get_columns_str, get_where_string, validate_alias_identifier,
        validate_table_name, validate_where_column,
    },
};
use crate::{db::connect, AppState, Error};

/// Returns the results of a `SELECT /*..*/ FROM {TABLE}` query.
pub fn select_table_rows(
    state: &AppState,
    params: QueryParamsSelect,
) -> impl Future<Item = Vec<RowValues>, Error = Error> {
    if let Err(e) = validate_table_name(&params.table) {
        return Either::A(err(e));
    }

    // get list of every column being used in the query params (columns, where, distinct, group_by,
    // order_by). Used for finding all foreign key references
    let columns_result: Result<Vec<String>, Error> = params
        .columns
        .par_iter()
        .map(|col| {
            if let Some((actual_column_ref, _alias)) = validate_alias_identifier(col)? {
                Ok(actual_column_ref.to_string())
            } else {
                Ok(col.to_string())
            }
        })
        .collect::<Result<Vec<String>, Error>>();

    let mut columns = match columns_result {
        Ok(columns) => columns,
        Err(e) => return Either::A(err(e)),
    };

    // WHERE clause w/ foreign key references
    let where_ast = match conditions_params_to_ast(&params.conditions) {
        Ok(ast) => ast,
        Err(e) => return Either::A(err(e)),
    };
    columns.par_extend(fk_columns_from_where_ast(&where_ast));

    if let Some(v) = &params.distinct {
        columns.par_extend(v.clone());
    }
    if let Some(v) = &params.group_by {
        columns.par_extend(v.clone());
    }
    if let Some(v) = &params.order_by {
        columns.par_extend(v.clone());
    }

    // get table stats for building query (we need to know the column types)
    let table_clone = params.table.clone();
    let stats_future =
        connect(state.config.db_url)
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
    .join(stats_future)
    .and_then(move |(fk_columns, stats)| {
        let (statement_str, prepared_values) =
            match build_select_statement(params, stats, fk_columns, where_ast) {
                Ok((stmt, prep_vals)) => (stmt, prep_vals),
                Err(e) => return Either::A(err(e)),
            };

        // sending prepared statement to postgres
        let select_rows_future = connect(&db_url_str)
            .map_err(Error::from)
            .and_then(move |mut conn| {
                conn.prepare(&statement_str)
                    .map_err(Error::from)
                    .and_then(move |statement| {
                        let prep_values: Vec<&dyn ToSql> =
                            prepared_values.iter().map(|v| v as _).collect();

                        conn.query(&statement, &prep_values)
                            .collect()
                            .map_err(Error::from)
                    })
            })
            .and_then(|rows| {
                match rows
                    .par_iter()
                    .map(row_to_row_values)
                    .collect::<Result<Vec<RowValues>, Error>>()
                {
                    Ok(row_values) => Ok(row_values),
                    Err(e) => Err(e),
                }
            });

        Either::B(select_rows_future)
    });

    Either::B(fk_future)
}

fn build_select_statement(
    params: QueryParamsSelect,
    stats: Vec<TableColumnStat>,
    fks: Vec<ForeignKeyReference>,
    mut where_ast: Expr,
) -> Result<(String, Vec<TypedColumnValue>), Error> {
    let mut statement = vec!["SELECT "];
    let is_fks_exist = !fks.is_empty();

    // DISTINCT clause if exists
    if let Some(distinct_columns) = &params.distinct {
        statement.push("DISTINCT ON (");

        statement.par_extend(get_columns_str(&distinct_columns, &params.table, &fks)?);

        statement.push(") ");
    }

    // building column selection
    statement.par_extend(get_columns_str(&params.columns, &params.table, &fks)?);

    statement.push(" FROM ");
    statement.push(&params.table);

    // build inner join expression
    let inner_join_str = if is_fks_exist {
        ForeignKeyReference::join_foreign_key_references(
            &fks,
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
            "\nINNER JOIN ",
        )
    } else {
        "".to_string()
    };
    if is_fks_exist {
        statement.push(" INNER JOIN ");
        statement.push(&inner_join_str);
    }

    // building WHERE string
    let (mut where_string, column_types) =
        get_where_string(&mut where_ast, &params.table, &stats, &fks);
    let mut prepared_values = vec![];
    if &where_string != "" {
        statement.push(" WHERE (");

        // parse through the `WHERE` AST and return a tuple: (expression-with-prepared-params
        // string, Vec of tuples (position, Value)).
        let (where_string_with_prepared_positions, prepared_values_vec) =
            TypedColumnValue::generate_prepared_statement_from_ast_expr(
                &where_ast,
                &params.table,
                &column_types,
                None,
            )?;
        where_string = where_string_with_prepared_positions;
        prepared_values = prepared_values_vec;

        statement.push(&where_string);
        statement.push(")");
    }

    // GROUP BY statement
    if let Some(group_by_columns) = &params.group_by {
        statement.push(" GROUP BY ");
        statement.par_extend(get_columns_str(group_by_columns, &params.table, &fks)?);
    }

    // Append ORDER BY if the param exists
    if let Some(order_by_columns) = &params.order_by {
        statement.push(" ORDER BY ");

        lazy_static! {
            // case-insensitive search for ORDER BY direction
            static ref ORDER_BY_DIRECTION_RE: Regex = Regex::new(r"(?i) asc| desc").unwrap();
        }

        for (i, column) in order_by_columns.iter().enumerate() {
            // using `is_match` first because it's faster than `find`
            let (sql_column, order_by_direction): (&str, &str) =
                if ORDER_BY_DIRECTION_RE.is_match(column) {
                    // separate the column string from the direction string
                    match ORDER_BY_DIRECTION_RE.find(column) {
                        Some(order_direction_match) => {
                            let order_by_column = &column[..order_direction_match.start()];
                            validate_where_column(order_by_column)?;

                            let order_by_direction = &column[order_direction_match.start()..];
                            (order_by_column, order_by_direction)
                        }
                        None => {
                            validate_where_column(column)?;
                            (column, " asc")
                        }
                    }
                } else {
                    validate_where_column(column)?;
                    (column, " asc")
                };

            if let (true, Some((fk_ref, fk_column))) = (
                is_fks_exist,
                ForeignKeyReference::find(&fks, &params.table, sql_column),
            ) {
                statement.push(fk_ref.foreign_key_table.as_str());
                statement.push(".");
                statement.push(fk_column);
            } else {
                statement.push(sql_column);
            }

            statement.push(if order_by_direction == " desc" {
                " DESC"
            } else {
                " ASC"
            });

            if i < order_by_columns.len() - 1 {
                statement.push(", ");
            }
        }
    }

    // LIMIT
    let limit_str = params.limit.to_string();
    statement.push(" LIMIT ");
    statement.push(&limit_str);

    // OFFSET
    let offset_str = params.offset.to_string();
    if params.offset > 0 {
        statement.push(" OFFSET ");
        statement.push(&offset_str);
    }

    statement.push(";");

    Ok((statement.join(""), prepared_values))
}

#[cfg(test)]
mod build_select_statement_tests {
    use super::*;
    use crate::queries::{postgres_types::IsNullColumnValue, query_types::QueryParamsSelect};
    use pretty_assertions::assert_eq;

    #[test]
    fn basic_query() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn multiple_columns() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string(), "name".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id, name FROM a_table LIMIT 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn distinct() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: Some(vec!["name".to_string(), "blah".to_string()]),
                group_by: None,
                limit: 100,
                offset: 0,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT DISTINCT ON (name, blah) id FROM a_table LIMIT 100;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn offset() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 1000,
                offset: 100,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(&sql, "SELECT id FROM a_table LIMIT 1000 OFFSET 100;");
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn order_by() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string()],
                conditions: None,
                distinct: None,
                group_by: None,
                limit: 1000,
                offset: 0,
                order_by: Some(vec!["name".to_string(), "test".to_string()]),
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table ORDER BY name ASC, test ASC LIMIT 1000;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn group_by_alias() {
        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["COUNT(id) AS id_count".to_string(), "name".to_string()],
                conditions: None,
                distinct: None,
                group_by: Some(vec!["name".to_string()]),
                limit: 1000,
                offset: 0,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![],
            vec![],
            Expr::Identifier("".to_string()),
        ) {
            Ok((sql, _)) => {
                assert_eq!(
                    &sql,
                    "SELECT COUNT(id) AS id_count, name FROM a_table GROUP BY name LIMIT 1000;"
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn conditions() {
        let conditions = "(id > 10 OR id < 20) AND name = 'test'";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();

        match build_select_statement(
            QueryParamsSelect {
                columns: vec!["id".to_string()],
                conditions: Some(conditions.to_string()),
                distinct: None,
                group_by: None,
                limit: 10,
                offset: 0,
                order_by: None,
                table: "a_table".to_string(),
            },
            vec![
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
            vec![],
            where_ast,
        ) {
            Ok((sql, prepared_values)) => {
                assert_eq!(
                    &sql,
                    "SELECT id FROM a_table WHERE ((a_table.id > $1 OR a_table.id < $2) AND a_table.name = $3) LIMIT 10;"
                );
                assert_eq!(
                    prepared_values,
                    vec![
                        TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(10)),
                        TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(20)),
                        TypedColumnValue::Text(IsNullColumnValue::NotNullable("test".to_string())),
                    ]
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }

    #[test]
    fn complex_query() {
        let conditions = "id = 46327143679919107 AND test_name = 'a name'";
        let where_ast = conditions_params_to_ast(&Some(conditions.to_string())).unwrap();

        match build_select_statement(
            QueryParamsSelect {
                columns: vec![
                    "id".to_string(),
                    "test_bigint".to_string(),
                    "test_bigserial".to_string(),
                ],
                conditions: Some(conditions.to_string()),
                distinct: Some(vec![
                    "test_date".to_string(),
                    "test_timestamptz".to_string(),
                ]),
                group_by: None,
                limit: 10000,
                offset: 2000,
                order_by: Some(vec!["due_date desc".to_string()]),
                table: "a_table".to_string(),
            },
            vec![
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
                    column_name: "test_bigint".to_string(),
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
                    column_name: "test_bigserial".to_string(),
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
                    column_name: "test_name".to_string(),
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
                    column_name: "test_date".to_string(),
                    column_type: "date",
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
                    column_name: "test_timestamptz".to_string(),
                    column_type: "timestamptz",
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
                    column_name: "due_date".to_string(),
                    column_type: "date",
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
            vec![],
            where_ast,
        ) {
            Ok((sql, prepared_values)) => {
                assert_eq!(
                    &sql,
                    "SELECT DISTINCT ON (test_date, test_timestamptz) id, test_bigint, test_bigserial FROM a_table WHERE (a_table.id = $1 AND a_table.test_name = $2) ORDER BY due_date DESC LIMIT 10000 OFFSET 2000;"
                );

                assert_eq!(
                    prepared_values,
                    vec![
                        TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(
                            46_327_143_679_919_107i64
                        )),
                        TypedColumnValue::Text(IsNullColumnValue::NotNullable(
                            "a name".to_string()
                        )),
                    ]
                );
            }
            Err(e) => {
                panic!(e);
            }
        };
    }
}
