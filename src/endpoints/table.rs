use actix_web::{AsyncResponder, FutureResponse, HttpRequest, HttpResponse};
use futures::Future;

use crate::errors::ApiError;
use crate::queries::query_types::{Query, QueryParams, QueryTasks};
use crate::AppState;

// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query: Query = Query {
        params: QueryParams::from_http_request(req),
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
    let params = QueryParams::from_http_request(req);

    let task = if params.columns.is_empty() {
        QueryTasks::QueryTableStats
    } else {
        QueryTasks::QueryTable
    };

    let query = Query { params, task };

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
