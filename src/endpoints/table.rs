use actix_web::dev::HttpResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{web, HttpRequest, HttpResponse};
use futures::future::{err, Either};
use futures::Future;
use serde_json::Value;

use crate::db::Pool;
use crate::errors::ApiError;
use crate::queries::{
    insert_into_table, query_types, select_all_tables, select_table_rows, select_table_stats,
};
use query_types::{QueryParamsInsert, QueryParamsSelect, RequestQueryStringParams};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    db: web::Data<Pool>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    db.connection()
        .map_err(ApiError::from)
        .and_then(|conn| select_all_tables(conn).map_err(ApiError::from))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Inserts new rows into a table
pub fn post_table(
    req: HttpRequest,
    db: web::Data<Pool>,
    body: Json<Value>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
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

    let insert_response = db
        .connection()
        .map_err(ApiError::from)
        .and_then(|conn| insert_into_table(conn, params))
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        })
        .or_else(|e| Err(ApiError::from(e)));

    Either::B(insert_response)
}

/// Queries a table using SELECT.
pub fn get_table(
    req: HttpRequest,
    db: web::Data<Pool>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    let params = QueryParamsSelect::from_http_request(&req, query_string_params.into_inner());

    if params.columns.is_empty() {
        Either::A(get_table_stats(db, params.table))
    } else {
        Either::B(get_table_rows(db, params))
    }
}

fn get_table_rows(
    db: web::Data<Pool>,
    params: QueryParamsSelect,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    select_table_rows(db.get_ref(), params)
        .map_err(ApiError::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
        .or_else(|e| Err(ApiError::from(e)))
}

fn get_table_stats(
    db: web::Data<Pool>,
    table: String,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    db.connection()
        .map_err(ApiError::from)
        .and_then(|conn| select_table_stats(conn, table))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
        .or_else(|e| Err(ApiError::from(e)))
}
