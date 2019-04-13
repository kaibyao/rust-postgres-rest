use super::postgres_types::RowFields;
use super::table_stats::TableStats;
use crate::errors::ApiError;
use crate::AppState;
use actix_web::{actix::Message, HttpRequest};
use serde_json::Value;

/// Represents a single database query
pub struct QueryParams {
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

impl QueryParams {
    pub fn from_http_request(req: &HttpRequest<AppState>) -> Self {
        let default_limit = 10000;
        let default_offset = 0;

        let query_params = req.query();

        QueryParams {
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

/// Represents a database task (w/ included query) to be performed by DbExecutor
pub struct Query {
    // pub req_body: Option<String>,
    pub params: QueryParams,
    pub req_body: Option<Value>,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, ApiError>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    CreateTable,
    GetAllTables,
    // InsertIntoTable,
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
    Success(bool),
    TableStats(TableStats),
}
