use actix_web::{
    dev::HttpResponseBuilder,
    http::StatusCode,
    web::{self, Json},
    HttpRequest, HttpResponse,
};
use futures::{
    future::{err, Either},
    Future,
};
use serde_json::Value;

use crate::{
    db::connect,
    queries::{
        insert_into_table, query_types, select_all_tables, select_table_rows, select_table_stats,
    },
    AppState, Error,
};
use query_types::{QueryParamsInsert, QueryParamsSelect, RequestQueryStringParams};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    state: web::Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    connect(state.config.db_url)
        .map_err(Error::from)
        .and_then(|client| select_all_tables(client).map_err(Error::from))
        .and_then(|(rows, _client)| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Inserts new rows into a table. Returns the number of rows affected.
pub fn post_table(
    req: HttpRequest,
    state: web::Data<AppState>,
    body: Json<Value>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let actual_body = body.into_inner();
    let params = match QueryParamsInsert::from_http_request(
        &req,
        actual_body,
        query_string_params.into_inner(),
    ) {
        Ok(insert_params) => insert_params,
        Err(e) => {
            return Either::A(err(e));
        }
    };

    let insert_response = connect(state.config.db_url)
        .map_err(Error::from)
        .and_then(|client| insert_into_table(client, params))
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        });

    Either::B(insert_response)
}

/// Queries a table using SELECT.
pub fn get_table(
    req: HttpRequest,
    state: web::Data<AppState>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let params = match QueryParamsSelect::from_http_request(&req, query_string_params.into_inner())
    {
        Ok(params) => params,
        Err(e) => return Either::A(err(e)),
    };

    if params.columns.is_empty() {
        Either::B(Either::A(get_table_stats(state, params.table)))
    } else {
        Either::B(Either::B(get_table_rows(state, params)))
    }
}

fn get_table_rows(
    state: web::Data<AppState>,
    params: QueryParamsSelect,
) -> impl Future<Item = HttpResponse, Error = Error> {
    select_table_rows(state.get_ref(), params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

fn get_table_stats(
    state: web::Data<AppState>,
    table: String,
) -> impl Future<Item = HttpResponse, Error = Error> {
    select_table_stats(state.get_ref(), table)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}
