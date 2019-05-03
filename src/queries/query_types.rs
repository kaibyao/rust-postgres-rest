use super::postgres_types::RowFields;
use super::table_stats::TableStats;
use crate::errors::ApiError;
use crate::AppState;
use actix_web::{actix::Message, HttpRequest};
use serde_json::{Map, Value};

/// Represents a single SELECT query
pub struct QueryParamsSelect {
    pub distinct: Option<String>,
    pub columns: Vec<String>,
    pub table: String,
    pub conditions: Option<String>,
    pub group_by: Option<String>,
    pub order_by: Option<String>,
    pub limit: i32,
    pub offset: i32,
    pub prepared_values: Option<String>,
}

impl QueryParamsSelect {
    /// Fills the struct’s values based on the HttpRequest data.
    pub fn from_http_request(req: &HttpRequest<AppState>) -> Self {
        let default_limit = 10000;
        let default_offset = 0;

        let query_params = req.query();

        QueryParamsSelect {
            columns: match query_params.get("columns") {
                Some(columns_str) => columns_str
                    .split(',')
                    .map(|column_str_raw| String::from(column_str_raw.trim()))
                    .collect(),
                None => vec![],
            },
            distinct: match query_params.get("distinct") {
                Some(distinct_string) => Some(distinct_string.clone()),
                None => None,
            },
            table: match req.match_info().query("table") {
                Ok(table_name) => table_name,
                Err(_) => "".to_string(),
            },
            conditions: match query_params.get("where") {
                Some(where_string) => Some(where_string.clone()),
                None => None,
            },
            group_by: match query_params.get("group_by") {
                Some(group_by_str) => Some(group_by_str.clone()),
                None => None,
            },
            order_by: match query_params.get("order_by") {
                Some(order_by_str) => Some(order_by_str.clone()),
                None => None,
            },
            limit: match query_params.get("limit") {
                Some(limit_string) => match limit_string.parse() {
                    Ok(limit_i32) => limit_i32,
                    Err(_) => default_limit,
                },
                None => default_limit,
            },
            offset: match query_params.get("offset") {
                Some(offset_string) => match offset_string.parse() {
                    Ok(offset_i32) => offset_i32,
                    Err(_) => default_offset,
                },
                None => default_offset,
            },
            prepared_values: match query_params.get("prepared_values") {
                Some(prepared_values) => Some(prepared_values.clone()),
                None => None,
            },
        }
    }
}

/// Represents a single SELECT query
pub struct QueryParamsInsert {
    pub is_upsert: bool,
    pub rows: Vec<Map<String, Value>>,
    pub table: String,
}

impl QueryParamsInsert {
    /// Fills the struct’s values based on the HttpRequest data.
    pub fn from_http_request(req: &HttpRequest<AppState>, body: Value) -> Result<Self, ApiError> {
        let table = match req.match_info().query("table") {
            Ok(table_name) => table_name,
            Err(_) => unreachable!("Not possible to reach this endpoint without a table name."),
        };

        let rows: Vec<Map<String, Value>> = match body.as_array() {
            Some(body_rows_to_insert) => {
                if !body_rows_to_insert
                .iter().all(Value::is_object) {
                    return Err(ApiError::generate_error("INCORRECT_REQUEST_BODY", "The body needs to be an array of objects where each object represents a row and whose key-values represent column names and their values.".to_string()));
                }

                body_rows_to_insert
                .iter().map(|json_value| {
                    if let Some(row_obj_map) = json_value.as_object() {
                        row_obj_map.clone()
                    } else {
                        unreachable!("Taken care of via above conditional.")
                    }
                })
                .collect()
            },
            None => return Err(ApiError::generate_error("INCORRECT_REQUEST_BODY", "The body needs to be an array of objects where each object represents a row and whose key-values represent column names and their values.".to_string())),
        };

        Ok(QueryParamsInsert {
            is_upsert: req.query().get("is_upsert").is_some(),
            rows,
            table,
        })
    }
}

pub enum QueryParams {
    Select(QueryParamsSelect),
    Insert(QueryParamsInsert),
}

/// Represents a database task (w/ included query) to be performed by DbExecutor
pub struct Query {
    pub params: QueryParams,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, ApiError>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTables,
    InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    QueryTable,
    QueryTableStats,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTablesResult(Vec<String>),
    QueryTableResult(Vec<RowFields>),
    RowsAffected(usize),
    TableStats(TableStats),
}
