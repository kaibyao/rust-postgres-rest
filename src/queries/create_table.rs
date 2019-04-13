use super::query_types::{Query, QueryResult};
use super::TableColumnStat;
use crate::db::Connection;
use crate::errors::{generate_error, ApiError};
use serde_json::Result as SerdeResult;

pub fn create_table(conn: &Connection, query: Query) -> Result<QueryResult, ApiError> {
    // parse columns of query body as table_stats::TableColumnStat
    if query.req_body == None {
        return Err(generate_error(
            "REQUIRED_PARAMETER_MISSING",
            "request body".to_string(),
        ));
    }
    let body = query.req_body.unwrap();

    let table_name_opt = &body["table_name"].as_str();
    if *table_name_opt == None {
        return Err(generate_error(
            "REQUIRED_PARAMETER_MISSING",
            "table_name".to_string(),
        ));
    }
    let table_name = table_name_opt.unwrap();

    let attempt_columnstat_conversion: SerdeResult<Vec<TableColumnStat>> =
        serde_json::from_value(body["columns"].clone());
    let columns = match attempt_columnstat_conversion {
        Ok(successfully_converted_columns) => successfully_converted_columns,
        Err(e) => {
            return Err(ApiError::from(e));
        }
    };

    // create CREATE TABLE string

    // execute and return result
    Ok(QueryResult::Success(true))
}
