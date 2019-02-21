use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
use futures::Future;

use crate::errors::ApiError;
use crate::queries::{
    query_types::{Query, QueryTasks},
    utils::validate_sql_name,
};
use crate::AppState;

// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query: Query = Query {
        columns: vec![],
        conditions: None,
        limit: 0,
        offset: 0,
        order_by: None,
        table: "".to_string(),
        task: QueryTasks::GetAllTables,
    };

    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(tables) => Ok(HttpResponse::Ok().json(tables)),
            Err(err) => Err(err),
        })
        .responder()
}

/// Queries a table using SELECT.
pub fn query_table(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let table: String = req.match_info().query("table").unwrap();

    let query_params = req.query();

    // set custom limit if available
    let default_limit = 10000;
    let limit: i32 = match query_params.get("limit") {
        Some(limit_string) => match limit_string.parse() {
            Ok(limit_i32) => limit_i32,
            Err(_) => default_limit,
        },
        None => default_limit,
    };

    // set custom offset if available
    let default_offset = 0;
    let offset: i32 = match query_params.get("offset") {
        Some(offset_string) => match offset_string.parse() {
            Ok(offset_i32) => offset_i32,
            Err(_) => default_offset,
        },
        None => default_offset,
    };

    // extract columns
    let columns: Vec<String> = match query_params.get("columns") {
        Some(columns_str) => columns_str
            .split(',')
            .map(|column_str_raw| String::from(column_str_raw.trim()))
            .collect(),
        None => vec![],
    };

    // extract ORDER BY
    let order_by: Option<String> = match query_params.get("order_by") {
        Some(order_by_str) => match validate_sql_name(order_by_str) {
            Ok(_) => Some(order_by_str.to_string()),
            Err(_) => None,
        },
        None => None,
    };

    let task = if columns.is_empty() {
        QueryTasks::QueryTableStats
    } else {
        QueryTasks::QueryTable
    };

    let query = Query {
        columns,
        conditions: None,
        limit,
        offset,
        order_by,
        table,
        task,
    };

    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
            Err(err) => Err(err),
        })
        .responder()
}
