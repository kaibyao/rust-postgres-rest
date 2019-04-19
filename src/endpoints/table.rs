use actix_web::{
    AsyncResponder, FromRequest, FutureResponse, HttpRequest, HttpResponse, Json, State,
};
use futures::{future, Future};
use serde_json::Value;

use crate::errors::ApiError;
use crate::queries::query_types::{
    Query, QueryParams, QueryParamsInsert, QueryParamsSelect, QueryTasks,
};
use crate::AppState;

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query: Query = Query {
        params: QueryParams::Select(QueryParamsSelect::from_http_request(req)),
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
            let actual_body = body.into_inner();
            let params = match QueryParamsInsert::from_http_request(&req, actual_body) {
                Ok(insert_params) => insert_params,
                Err(e) => {
                    return Box::from(future::err(e));
                }
            };

            let query: Query = Query {
                params: QueryParams::Insert(params),
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
