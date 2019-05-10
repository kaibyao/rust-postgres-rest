use super::{
    postgres_types::{convert_json_value_to_postgres, ColumnTypeValue},
    query_types::{Query, QueryParams, QueryParamsInsert, QueryResult},
    table_stats::get_column_stats,
};
use crate::db::Connection;
use crate::errors::ApiError;
use postgres::types::ToSql;
use serde_json::{Map, Value};
use std::collections::HashMap;

static INSERT_ROWS_BATCH_COUNT: usize = 2;

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

fn execute_insert<'a>(
    conn: &Connection,
    rows: &'a [Map<String, Value>],
    table: &str,
) -> Result<usize, ApiError> {
    // parse out the columns that have values to assign
    let columns = get_all_columns_to_insert(rows);

    dbg!(&columns);

    // TODO: figure out is_upsert

    // create initial prepared statement
    let num_rows = rows.len();
    let num_columns = columns.len();
    let values_params_str = get_prepared_statement_params_str(num_rows, num_columns);

    let insert_query_str = [
        "INSERT INTO ",
        table,
        &[" (", &columns.join(", "), ")"].join(""),
        " VALUES \n",
        &values_params_str,
    ]
    .join("");

    dbg!(&insert_query_str);

    let prep_statement = conn.prepare(&insert_query_str)?;

    // OK, apparently serde_json::Values can't automatically convert to non-JSON/JSONB columns. We need to actually get column types of the table we're inserting into so we know what type to convert each value into.
    let column_stats = get_column_stats(conn, table)?;
    let mut column_types: HashMap<String, String> = HashMap::new();
    for stat in column_stats.into_iter() {
        column_types.insert(stat.column_name, stat.column_type);
    }

    // create the vector of "values" query string (use DEFAULT for the columns that don't have a value in that row)

    // we are using this enum to denote whether an inserted row's column value is given (otherwise DEFAULT)
    enum ColumnTypeValueToInsert {
        Default,
        Value(ColumnTypeValue),
    }

    // generate the array of json-converted-to-rust_postgres values to insert.
    let nested_column_type_values_result: Result<Vec<Vec<ColumnTypeValueToInsert>>, ApiError> =
        rows.iter()
            .map(|row| -> Result<Vec<ColumnTypeValueToInsert>, ApiError> {
                columns
                    .iter()
                    .map(|column| match row.get(*column) {
                        Some(val) => {
                            let column_type = &column_types[*column];
                            match convert_json_value_to_postgres(column_type, val) {
                                Ok(column_type_value) => {
                                    Ok(ColumnTypeValueToInsert::Value(column_type_value))
                                }
                                Err(e) => Err(e),
                            }
                        }
                        None => Ok(ColumnTypeValueToInsert::Default),
                    })
                    .collect()
            })
            .collect();
    let nested_column_type_values = match nested_column_type_values_result {
        Ok(result) => result,
        Err(e) => return Err(e),
    };

    // flatten!
    let column_type_values: Vec<ColumnTypeValueToInsert> =
        nested_column_type_values.into_iter().flatten().collect();

    // convert the column values into the actual values we will use for the INSERT statement execution
    let mut prep_values: Vec<&ToSql> = vec![];
    for column_type_value_to_insert in column_type_values.iter() {
        match column_type_value_to_insert {
            ColumnTypeValueToInsert::Default => prep_values.push(&"DEFAULT"),
            ColumnTypeValueToInsert::Value(column_type_value) => match column_type_value {
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
            },
        };
    }

    dbg!(&prep_values);

    // execute sql & return results
    let results = prep_statement.query(&prep_values)?;
    Ok(results.len())
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

fn get_prepared_statement_params_str(num_rows: usize, num_columns: usize) -> String {
    let mut prep_column_number = 1;
    let mut row_strs = vec![];

    for _ in 0..num_rows {
        let mut row_str_arr = vec![];
        for _ in 0..num_columns {
            row_str_arr.push(format!("${}", prep_column_number));
            prep_column_number += 1;
        }
        row_strs.push(format!("({})", row_str_arr.join(", ")));
    }

    row_strs.join(",\n")
}
