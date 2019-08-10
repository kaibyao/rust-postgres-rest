use actix_web::{
    dev::HttpResponseBuilder,
    http::StatusCode,
    web::{self, Json},
    HttpMessage, HttpRequest, HttpResponse,
};
use futures::{
    future::{err, Either},
    Future,
};
use serde_json::{json, Value};

use super::query_params_from_request::{
    generate_delete_params_from_http_request, generate_insert_params_from_http_request,
    generate_select_params_from_http_request, generate_update_params_from_http_request,
    RequestQueryStringParams,
};
use crate::{Config, Error};
use rust_postgres_rest::{
    connect,
    queries::{
        delete_table_rows, execute_sql_query, insert_into_table,
        query_types::{ExecuteParams, SelectParams},
        select_all_tables, select_table_rows, select_table_stats, update_table_rows,
    },
};

/// Deletes table rows and optionally returns the column data in the deleted rows.
pub fn delete_table(
    req: HttpRequest,
    state: web::Data<Config>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let params =
        match generate_delete_params_from_http_request(&req, query_string_params.into_inner()) {
            Ok(params) => params,
            Err(e) => return Either::A(err(e)),
        };

    if params.confirm_delete.is_none() {
        return Either::A(err(Error::generate_error(
            "REQUIRED_PARAMETER_MISSING",
            "URL query parameter `confirm_delete` is necessary for table row deletion.".to_string(),
        )));
    }

    let delete_table_future = delete_table_rows(&state.get_ref().inner, params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(delete_table_future)
}

/// Executes the given SQL statement
pub fn execute_sql(
    req: HttpRequest,
    body: String,
    state: web::Data<Config>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let content_type = req.content_type().to_lowercase();
    if &content_type != "text/plain" {
        return Either::A(err(Error::generate_error(
            "INVALID_CONTENT_TYPE",
            format!("Content type sent was: `{}`.", content_type),
        )));
    }

    let params = ExecuteParams {
        statement: body,
        is_return_rows: query_string_params.is_return_rows.is_some(),
    };

    let execute_sql_future = connect(state.inner.db_url)
        .map_err(Error::from)
        .and_then(|client| execute_sql_query(client, params).map_err(Error::from))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(execute_sql_future)
}

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    state: web::Data<Config>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    connect(state.inner.db_url)
        .map_err(Error::from)
        .and_then(|client| select_all_tables(client).map_err(Error::from))
        .and_then(|(rows, _client)| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Queries a table using SELECT.
pub fn get_table(
    req: HttpRequest,
    state: web::Data<Config>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let params =
        match generate_select_params_from_http_request(&req, query_string_params.into_inner()) {
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
    state: web::Data<Config>,
    params: SelectParams,
) -> impl Future<Item = HttpResponse, Error = Error> {
    select_table_rows(&state.get_ref().inner, params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

fn get_table_stats(
    state: web::Data<Config>,
    table: String,
) -> impl Future<Item = HttpResponse, Error = Error> {
    select_table_stats(&state.get_ref().inner, table)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Inserts new rows into a table. Returns the number of rows affected.
pub fn post_table(
    req: HttpRequest,
    state: web::Data<Config>,
    body: Option<Json<Value>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let actual_body = match body {
        Some(body) => body.into_inner(),
        None => return Either::A(err(Error::generate_error("INCORRECT_REQUEST_BODY", "Request body is required. Body must be a JSON array of objects where each object represents a row and whose key-values represent column names and their values.".to_string())))
    };
    let params = match generate_insert_params_from_http_request(
        &req,
        actual_body,
        query_string_params.into_inner(),
    ) {
        Ok(insert_params) => insert_params,
        Err(e) => {
            return Either::A(err(e));
        }
    };

    let insert_response = connect(state.inner.db_url)
        .map_err(Error::from)
        .and_then(|client| insert_into_table(client, params).map_err(Error::from))
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        });

    Either::B(insert_response)
}

/// Runs an UPDATE query and returns either rows affected or row columns if specified.
pub fn put_table(
    req: HttpRequest,
    state: web::Data<Config>,
    body: Option<Json<Value>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let actual_body = match body {
        Some(body) => body.into_inner(),
        None => return Either::A(err(Error::generate_error("INCORRECT_REQUEST_BODY", "Request body is required. Body must be a JSON object whose key-values represent column names and the values to set. String values must contain quotes or else they will be evaluated as expressions and not strings.".to_string())))
    };

    if actual_body == json!({}) {
        return Either::A(err(Error::generate_error("INCORRECT_REQUEST_BODY", "Request body cannot be empty. Body must be a JSON object whose key-values represent column names and the values to set. String values must contain quotes or else they will be evaluated as expressions and not strings.".to_string())));
    }

    let params = match generate_update_params_from_http_request(
        &req,
        actual_body,
        query_string_params.into_inner(),
    ) {
        Ok(params) => params,
        Err(e) => {
            return Either::A(err(e));
        }
    };

    let response = update_table_rows(&state.get_ref().inner, params)
        .map_err(Error::from)
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        });

    Either::B(response)
}

/// Resets all caches (currently only Table Stats)
pub fn reset_caches(state: web::Data<Config>) -> impl Future<Item = HttpResponse, Error = Error> {
    state
        .get_ref()
        .inner
        .reset_cache()
        .then(|result| match result {
            Ok(_) => Ok(HttpResponseBuilder::new(StatusCode::OK).finish()),
            Err(e) => Err(Error::from(e)),
        })
}
