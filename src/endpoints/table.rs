use actix_web::{
    AsyncResponder, FromRequest, FutureResponse, HttpRequest, HttpResponse, Json, State,
};
use futures::Future;
use serde_json::Value;

use crate::errors::ApiError;
use crate::queries::query_types::{Query, QueryParams, QueryParamsSelect, QueryTasks};
use crate::AppState;

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query: Query = Query {
        params: QueryParams::Select(QueryParamsSelect::from_http_request(req)),
        req_body: None,
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

/// Inserts new rows into a table
pub fn insert_into_table(
    (req, state): (HttpRequest<AppState>, State<AppState>),
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Json::<Value>::extract(&req).from_err().and_then(
        move |body| -> FutureResponse<HttpResponse, ApiError> {
            // need to rethink how to pass unique endpoint params, QueryParams isn't gonna cut it.
            let query: Query = Query {
                params: QueryParams::Select(QueryParamsSelect::from_http_request(&req)),
                req_body: Some(body.into_inner()),
                task: QueryTasks::InsertIntoTable,
            };

            state
                .db
                .send(query)
                .from_err()
                .and_then(|res| match res {
                    Ok(_) => Ok(HttpResponse::Ok().into()),
                    Err(err) => Err(err),
                })
                .responder()
        },
    )
}

/// Queries a table using SELECT.
pub fn query_table(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let params = QueryParamsSelect::from_http_request(req);

    let task = if params.columns.is_empty() {
        QueryTasks::QueryTableStats
    } else {
        QueryTasks::QueryTable
    };

    let query = Query {
        params: QueryParams::Select(params),
        req_body: None,
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
