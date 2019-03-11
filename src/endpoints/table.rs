use actix_web::{
    AsyncResponder, FromRequest, FutureResponse, HttpRequest, HttpResponse, Json, State,
};
use futures::Future;
use serde_json::Value;

use crate::errors::ApiError;
use crate::queries::query_types::{Query, QueryParams, QueryTasks};
use crate::AppState;

/// Create a new table
pub fn create_table(
    (req, state): (HttpRequest<AppState>, State<AppState>),
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    Json::<Value>::extract(&req).from_err().and_then(
        move |body| -> FutureResponse<HttpResponse, ApiError> {
            dbg!(body);

            let query: Query = Query {
                params: QueryParams::from_http_request(&req),
                // req_body: req, // need to get the body string
                task: QueryTasks::CreateTable,
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

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, ApiError> {
    let query: Query = Query {
        params: QueryParams::from_http_request(req),
        // request: req,
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

    let query = Query {
        params,
        // request: req,
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
