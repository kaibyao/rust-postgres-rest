use super::{
    postgres_types::ColumnTypeValue,
    query_types::{Query, QueryParams, QueryParamsInsert, QueryResult},
    table_stats::get_column_stats,
};
use crate::db::Connection;
use crate::errors::ApiError;
use postgres::types::ToSql;
use serde_json::{Map, Value};
use std::collections::HashMap;

static INSERT_ROWS_BATCH_COUNT: usize = 2;

/// Runs an INSERT INTO <table> query
pub fn insert_into_table(conn: &Connection, query: Query) -> Result<QueryResult, ApiError> {
    // extract query data
    let mut query_params: QueryParamsInsert;
    match query.params {
        QueryParams::Insert(insert_params) => query_params = insert_params,
        _ => unreachable!("insert_into_table() should not be called without Insert parameter."),
    };

    // TODO: use a transaction instead of individual executes

    let num_rows = query_params.rows.len();
    let mut total_num_rows_affected = 0;
    if num_rows >= INSERT_ROWS_BATCH_COUNT {
        // batch inserts into groups of 100 (see https://www.depesz.com/2007/07/05/how-to-insert-data-to-database-as-fast-as-possible/)
        let mut batch_rows = vec![];
        for (i, row) in query_params.rows.into_iter().enumerate() {
            batch_rows.push(row);

            if (i + 1) % INSERT_ROWS_BATCH_COUNT == 0 || i == num_rows - 1 {
                // do batch inserts on pushed rows
                match execute_insert(conn, &batch_rows, &query_params.table) {
                    Ok(num_rows_affected) => total_num_rows_affected += num_rows_affected,
                    Err(e) => return Err(e),
                };

                // reset batch
                batch_rows.truncate(0);
            }
        }
    } else {
        // insert all rows
        match execute_insert(conn, &query_params.rows, &query_params.table) {
            Ok(num_rows_affected) => total_num_rows_affected = num_rows_affected,
            Err(e) => return Err(e),
        }
    }

    Ok(QueryResult::RowsAffected(total_num_rows_affected))
}

/// Runs the actual setting up + execution of the INSERT query
fn execute_insert<'a>(
    conn: &Connection,
    rows: &'a [Map<String, Value>],
    table: &str,
) -> Result<usize, ApiError> {
    // TODO: figure out is_upsert

    // parse out the columns that have values to assign
    let columns = get_all_columns_to_insert(rows);

    dbg!(&columns);

    // OK, apparently serde_json::Values can't automatically convert to non-JSON/JSONB columns. We need to actually get column types of the table we're inserting into so we know what type to convert each value into.
    let column_stats = get_column_stats(conn, table)?;
    let mut column_types: HashMap<String, String> = HashMap::new();
    for stat in column_stats.into_iter() {
        column_types.insert(stat.column_name, stat.column_type);
    }

    let (values_params_str, column_values) = get_insert_params(rows, &columns, column_types)?;

    // create initial prepared statement
    let insert_query_str = [
        "INSERT INTO ",
        table,
        &[" (", &columns.join(", "), ")"].join(""),
        " VALUES ",
        &values_params_str,
    ]
    .join("");

    dbg!(&insert_query_str);

    let prep_statement = conn.prepare(&insert_query_str)?;

    // convert the column values into the actual values we will use for the INSERT statement execution
    let mut prep_values: Vec<&ToSql> = vec![];
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

    dbg!(&prep_values);

    // execute sql & return results
    let results = prep_statement.query(&prep_values)?;
    Ok(results.len())
    // match prep_statement.query(&prep_values) {
    //     Ok(results) => Ok(results.len()),
    //     Err(e) => {
    //         dbg!(&e);
    //         Err(ApiError::from(e))
    //     }
    // }
}

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

/// Returns a Result containing the tuple that contains (the VALUES parameter string, the array of parameter values)
fn get_insert_params(
    rows: &[Map<String, Value>],
    columns: &[&str],
    column_types: HashMap<String, String>,
) -> Result<(String, Vec<ColumnTypeValue>), ApiError> {
    let mut prep_column_number = 1;
    let mut row_strs = vec![];

    // generate the array of json-converted-to-rust_postgres values to insert.
    let nested_column_values_result: Result<Vec<Vec<ColumnTypeValue>>, ApiError> = rows
        .iter()
        .map(|row| -> Result<Vec<ColumnTypeValue>, ApiError> {
            // row_str_arr is used for the prepared statement parameter string
            let mut row_str_arr: Vec<String> = vec![];
            let mut column_values: Vec<ColumnTypeValue> = vec![];

            for column in columns.iter() {
                // if the "row" json object has a value for column, then use the rust-converted value, otherwise use the column’s DEFAULT value
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
