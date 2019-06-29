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
    Error,
    queries::{
        insert_into_table, query_types, select_all_tables, select_table_rows, select_table_stats,
    },
    AppConfig,
};
use query_types::{QueryParamsInsert, QueryParamsSelect, RequestQueryStringParams};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    config: web::Data<AppConfig>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    connect(config.db_url)
        .map_err(Error::from)
        .and_then(|client| select_all_tables(client).map_err(Error::from))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Inserts new rows into a table
pub fn post_table(
    req: HttpRequest,
    config: web::Data<AppConfig>,
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

    let insert_response = connect(config.db_url)
        .map_err(Error::from).and_then(|client| insert_into_table(client, params)).and_then(|num_rows_affected| {
        Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
    });

    Either::B(insert_response)
}

/// Queries a table using SELECT.
pub fn get_table(
    req: HttpRequest,
    config: web::Data<AppConfig>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let params = QueryParamsSelect::from_http_request(&req, query_string_params.into_inner());

    if params.columns.is_empty() {
        Either::A(get_table_stats(config.db_url, params.table))
    } else {
        Either::B(get_table_rows(config.db_url, params))
    }
}

fn get_table_rows(
    db_url: &str,
    params: QueryParamsSelect,
) -> impl Future<Item = HttpResponse, Error = Error> {
    select_table_rows(db_url.to_string(), params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

fn get_table_stats(
    db_url: &str,
    table: String,
) -> impl Future<Item = HttpResponse, Error = Error> {
    connect(db_url)
        .map_err(Error::from)
        .and_then(|conn| select_table_stats(conn, table))
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}
