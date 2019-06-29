use futures::{
    future::{err, loop_fn, Either, Future, Loop},
    stream::Stream,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio_postgres::{types::ToSql, Client};

use super::{
    postgres_types::{convert_row_fields, ColumnTypeValue, RowFields},
    query_types::{QueryParamsInsert, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement},
};
use crate::Error;

static INSERT_ROWS_BATCH_COUNT: usize = 100;

enum InsertResult {
    Rows(Vec<RowFields>),
    NumRowsAffected(u64),
}

/// Runs an INSERT INTO <table> query
pub fn insert_into_table(
    mut conn: Client,
    params: QueryParamsInsert,
) -> impl Future<Item = QueryResult, Error = Error> {
    // serde_json::Values can't automatically convert to non-JSON/JSONB columns.
    // Therefore, get column types of table so we know what types into which the json values are
    // converted. apparently rust_postgres already does this in the background, would be nice if
    // there was a way to hook into existing functionality...

    // used in futures later
    let table = params.table.clone();

    select_column_stats_statement(&mut conn, &table)
        .map_err(Error::from)
        .and_then(move |statement| {
            let q = conn.query(&statement, &[]);
            select_column_stats(q)
                .map_err(Error::from)
                .map(|stats| (stats, conn))
        })
        .and_then(move |(stats, mut conn)| {
            let mut column_types: HashMap<String, String> = HashMap::new();

            for stat in stats.into_iter() {
                column_types.insert(stat.column_name, stat.column_type);
            }

            let num_rows = params.rows.len();
            if num_rows > INSERT_ROWS_BATCH_COUNT {
                // batch inserts into groups of 100 (see https://www.depesz.com/2007/07/05/how-to-insert-data-to-database-as-fast-as-possible/)
                let mut batch_rows = vec![];
                let mut insert_batches = vec![];
                for (i, row) in params.rows.iter().enumerate() {
                    batch_rows.push(row.clone());

                    if (i + 1) % INSERT_ROWS_BATCH_COUNT == 0 || i == num_rows - 1 {
                        // do batch inserts on pushed rows
                        insert_batches.push(batch_rows);
                        batch_rows = vec![];
                    }
                }

                let batch_insert_future = conn
                    .simple_query("BEGIN")
                    .collect()
                    .then(|r| match r {
                        Ok(_) => Ok(conn),
                        Err(e) => Err((Error::from(e), conn)),
                    })
                    .and_then(|conn| {
                        loop_fn(
                            (0, 0, vec![], column_types, insert_batches, params, conn),
                            |(
                                i,
                                mut total_num_rows_affected,
                                mut total_rows_returned,
                                column_types,
                                insert_batches,
                                params,
                                conn,
                            )| {
                                execute_insert(conn, params, column_types, &insert_batches[i])
                                    .and_then(move |(conn, params, column_types, insert_result)| {
                                        match insert_result {
                                            InsertResult::NumRowsAffected(num_rows_affected) => {
                                                total_num_rows_affected += num_rows_affected;
                                            }
                                            InsertResult::Rows(rows) => {
                                                total_rows_returned.extend(rows);
                                            }
                                        };

                                        if i == insert_batches.len() - 1 {
                                            Ok(Loop::Break((
                                                total_num_rows_affected,
                                                total_rows_returned,
                                                params,
                                                conn,
                                            )))
                                        } else {
                                            Ok(Loop::Continue((
                                                i + 1,
                                                total_num_rows_affected,
                                                total_rows_returned,
                                                column_types,
                                                insert_batches,
                                                params,
                                                conn,
                                            )))
                                        }
                                    })
                            },
                        )
                    })
                    .map(
                        |(total_num_rows_affected, total_rows_returned, params, conn)| {
                            ((total_num_rows_affected, total_rows_returned, params), conn)
                        },
                    )
                    .and_then(|(results, mut conn)| {
                        conn.simple_query("COMMIT")
                            .for_each(|_| Ok(()))
                            .then(|r| match r {
                                Ok(_) => Ok(results),
                                Err(e) => Err((Error::from(e), conn)),
                            })
                    })
                    .or_else(|(e, mut conn)| {
                        conn.simple_query("ROLLBACK")
                            .for_each(|_| Ok(()))
                            .then(|_| Err(e))
                    })
                    .and_then(|(total_num_rows_affected, total_rows_returned, params)| {
                        if params.returning_columns.is_some() {
                            Ok(QueryResult::QueryTableResult(total_rows_returned))
                        } else {
                            Ok(QueryResult::from_num_rows_affected(total_num_rows_affected))
                        }
                    });

                Either::A(batch_insert_future)
            } else {
                // insert all rows
                let rows = params.rows.clone();
                let simple_insert_future =
                    execute_insert(conn, params, column_types, &rows).then(|result| match result {
                        Ok((_conn, _params, _column_types, insert_result)) => match insert_result {
                            InsertResult::NumRowsAffected(num_rows_affected) => {
                                Ok(QueryResult::from_num_rows_affected(num_rows_affected))
                            }
                            InsertResult::Rows(rows) => Ok(QueryResult::QueryTableResult(rows)),
                        },
                        Err((e, _client)) => Err(e),
                    });

                // simple_insert_future
                Either::B(simple_insert_future)
            }
        })
}

/// Runs the actual setting up + execution of the INSERT query
fn execute_insert<'a>(
    mut conn: Client,
    params: QueryParamsInsert,
    column_types: HashMap<String, String>,
    rows: &'a [Map<String, Value>],
) -> impl Future<
    Item = (
        Client,
        QueryParamsInsert,
        HashMap<String, String>,
        InsertResult,
    ),
    Error = (Error, Client),
> {
    let mut is_return_rows = false;

    // parse out the columns that have values to assign
    let columns = get_all_columns_to_insert(rows);
    let (values_params_str, column_values) = match get_insert_params(rows, &columns, &column_types)
    {
        Ok((values_params_str, column_values)) => (values_params_str, column_values),
        Err(e) => return Either::A(err((e, conn))),
    };

    // generate the ON CONFLICT string
    let conflict_clause = match generate_conflict_str(&params, &columns) {
        Some(conflict_str) => conflict_str,
        None => "".to_string(),
    };

    // generate the RETURNING string
    let returning_clause = match generate_returning_clause(&params) {
        Some(returning_str) => {
            is_return_rows = true;
            returning_str
        }
        None => "".to_string(),
    };

    // create initial prepared statement
    let insert_query_str = [
        "INSERT INTO ",
        &params.table,
        &[" (", &columns.join(", "), ")"].join(""),
        " VALUES ",
        &values_params_str,
        &conflict_clause,
        &returning_clause,
    ]
    .join("");

    let insert_future = conn
        .prepare(&insert_query_str)
        .then(move |result| match result {
            Ok(statement) => Ok((statement, conn)),
            Err(e) => Err((Error::from(e), conn)),
        })
        .and_then(move |(statement, mut conn)| {
            // convert the column values into the actual values we will use for the INSERT statement
            // execution
            let mut prep_values: Vec<&dyn ToSql> = vec![];
            for column_value in column_values.iter() {
                match column_value {
                    ColumnTypeValue::BigInt(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Bool(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::ByteA(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Char(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Citext(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Date(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Decimal(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Float8(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::HStore(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Int(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Json(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::JsonB(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::MacAddr(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Name(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Oid(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Real(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::SmallInt(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Text(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Time(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Timestamp(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::TimestampTz(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::Uuid(col_val) => prep_values.push(col_val),
                    ColumnTypeValue::VarChar(col_val) => prep_values.push(col_val),
                };
            }

            if is_return_rows {
                let return_rows_future = conn.query(&statement, &prep_values).collect().then(
                    move |result| match result {
                        Ok(rows) => {
                            match rows
                                .iter()
                                .map(|row| convert_row_fields(&row))
                                .collect::<Result<Vec<RowFields>, Error>>()
                            {
                                Ok(row_fields) => {
                                    Ok((conn, params, column_types, InsertResult::Rows(row_fields)))
                                }
                                Err(e) => Err((e, conn)),
                            }
                        }
                        Err(e) => Err((Error::from(e), conn)),
                    },
                );

                Either::A(return_rows_future)
            } else {
                let return_row_count_future =
                    conn.execute(&statement, &prep_values)
                        .then(move |result| match result {
                            Ok(num_rows) => Ok((
                                conn,
                                params,
                                column_types,
                                InsertResult::NumRowsAffected(num_rows),
                            )),
                            Err(e) => Err((Error::from(e), conn)),
                        });

                Either::B(return_row_count_future)
            }
        });

    Either::B(insert_future)
}

/// Generates the ON CONFLICT clause. If conflict action is "nothing", then "DO NOTHING" is
/// returned. If conflict action is "update", then sets all columns that aren't conflict target
/// columns to the excluded row's column value.
fn generate_conflict_str(params: &QueryParamsInsert, columns: &[&str]) -> Option<String> {
    if let (Some(conflict_action_str), Some(conflict_target_vec)) =
        (&params.conflict_action, &params.conflict_target)
    {
        // filter out any conflict target columns and convert the remaining columns into "SET <col>
        // = EXCLUDED.<col>" clauses
        let expanded_conflict_action = if conflict_action_str == "update" {
            [
                "DO UPDATE SET ",
                &columns
                    .iter()
                    .filter_map(|col| {
                        match conflict_target_vec
                            .iter()
                            .position(|conflict_target| conflict_target == *col)
                        {
                            Some(_) => None,
                            None => Some([*col, "=", "EXCLUDED.", *col].join("")),
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(", "),
            ]
            .join("")
        } else {
            "DO NOTHING".to_string()
        };

        return Some(
            [
                " ON CONFLICT (",
                &conflict_target_vec.join(", "),
                ") ",
                &expanded_conflict_action,
            ]
            .join(""),
        );
    }

    None
}

fn generate_returning_clause(params: &QueryParamsInsert) -> Option<String> {
    if let Some(returning_columns) = &params.returning_columns {
        return Some([" RETURNING ", &returning_columns.join(", ")].join(""));
    }

    None
}

/// Searches all rows being inserted and returns a vector containing all of the column names
fn get_all_columns_to_insert<'a>(rows: &'a [Map<String, Value>]) -> Vec<&'a str> {
    // parse out the columns that have values to assign
    let mut columns: Vec<&str> = vec![];
    for row in rows.iter() {
        for column in row.keys() {
            if columns.iter().position(|&c| c == column).is_none() {
                columns.push(column);
            }
        }
    }
    columns
}

/// Returns a Result containing the tuple that contains (the VALUES parameter string, the array of
/// parameter values)
fn get_insert_params(
    rows: &[Map<String, Value>],
    columns: &[&str],
    column_types: &HashMap<String, String>,
) -> Result<(String, Vec<ColumnTypeValue>), Error> {
    let mut prep_column_number = 1;
    let mut row_strs = vec![];

    // generate the array of json-converted-to-rust_postgres values to insert.
    let nested_column_values_result: Result<Vec<Vec<ColumnTypeValue>>, Error> = rows
        .iter()
        .map(|row| -> Result<Vec<ColumnTypeValue>, Error> {
            // row_str_arr is used for the prepared statement parameter string
            let mut row_str_arr: Vec<String> = vec![];
            let mut column_values: Vec<ColumnTypeValue> = vec![];

            for column in columns.iter() {
                // if the "row" json object has a value for column, then use the rust-converted
                // value, otherwise use the column’s DEFAULT value
                match row.get(*column) {
                    Some(val) => {
                        let prep_column_number_str =
                            ["$", &prep_column_number.to_string()].join("");
                        row_str_arr.push(prep_column_number_str);
                        prep_column_number += 1;

                        let column_type = &column_types[*column];
                        match ColumnTypeValue::from_json(column_type, val) {
                            Ok(column_type_value) => {
                                column_values.push(column_type_value);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    None => {
                        row_str_arr.push("DEFAULT".to_string());
                    }
                };
            }

            row_strs.push(format!("({})", row_str_arr.join(", ")));
            Ok(column_values)
        })
        .collect();

    let values_str = row_strs.join(", ");

    let nested_column_values = nested_column_values_result?;
    let column_values: Vec<ColumnTypeValue> = nested_column_values.into_iter().flatten().collect();

    Ok((values_str, column_values))
}
