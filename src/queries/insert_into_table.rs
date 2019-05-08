use super::{
    postgres_types::convert_json_value_to_rust,
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
    let default_str = "DEFAULT".to_string();
    let prep_values: Vec<&ToSql> = rows
        .iter()
        .map(|row| {
            let row_values: Vec<&ToSql> = columns
                .iter()
                .map(|column| -> &ToSql {
                    match row.get(*column) {
                        Some(val) => {
                            let column_type = &column_types[*column];
                            // convert_json_value_to_rust(column_type, val)
                            val
                        }
                        None => &default_str,
                    }
                })
                .collect();
            row_values
        })
        .flatten()
        .collect();

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
