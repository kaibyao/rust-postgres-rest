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
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use super::query_params_from_request::{
    generate_delete_params_from_http_request, generate_insert_params_from_http_request,
    generate_select_params_from_http_request, generate_update_params_from_http_request,
    RequestQueryStringParams,
};
use crate::{Config, Error};
use postgres_rest::queries;

/// Deletes table rows and optionally returns the column data in the deleted rows.
pub fn delete_table<T>(
    req: HttpRequest,
    config: web::Data<Config<T>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
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

    let delete_table_future = queries::delete_table_rows(config.get_ref().inner.clone(), params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(delete_table_future)
}

/// Executes the given SQL statement
pub fn execute_sql<T>(
    req: HttpRequest,
    body: String,
    config: web::Data<Config<T>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    let content_type = req.content_type().to_lowercase();
    if &content_type != "text/plain" {
        return Either::A(err(Error::generate_error(
            "INVALID_CONTENT_TYPE",
            format!("Content type sent was: `{}`.", content_type),
        )));
    }

    let params = queries::ExecuteParams {
        statement: body,
        is_return_rows: query_string_params.is_return_rows.is_some(),
    };

    let execute_sql_future = config
        .connect()
        .map_err(Error::from)
        .and_then(|client| queries::execute_sql_query(client, params).map_err(Error::from))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(execute_sql_future)
}

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names<T>(
    config: web::Data<Config<T>>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    config
        .connect()
        .map_err(Error::from)
        .and_then(|client| queries::select_all_tables(client).map_err(Error::from))
        .and_then(|(rows, _client)| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Queries a table using SELECT.
pub fn get_table<T>(
    req: HttpRequest,
    config: web::Data<Config<T>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    let params =
        match generate_select_params_from_http_request(&req, query_string_params.into_inner()) {
            Ok(params) => params,
            Err(e) => return Either::A(err(e)),
        };

    if params.columns.is_empty() {
        Either::B(Either::A(get_table_stats(config, params.table)))
    } else {
        Either::B(Either::B(get_table_rows(config, params)))
    }
}

fn get_table_rows<T>(
    config: web::Data<Config<T>>,
    params: queries::SelectParams,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    queries::select_table_rows(config.get_ref().inner.clone(), params)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

fn get_table_stats<T>(
    config: web::Data<Config<T>>,
    table: String,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    // <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    // <T as MakeTlsConnect<Socket>>::Stream: Send,
    // <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    queries::select_table_stats(&config.get_ref().inner, table)
        .map_err(Error::from)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
}

/// Inserts new rows into a table. Returns the number of rows affected.
pub fn post_table<T>(
    req: HttpRequest,
    config: web::Data<Config<T>>,
    body: Option<Json<Value>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
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

    let insert_response = config
        .connect()
        .map_err(Error::from)
        .and_then(|client| queries::insert_into_table(client, params).map_err(Error::from))
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        });

    Either::B(insert_response)
}

/// Runs an UPDATE query and returns either rows affected or row columns if specified.
pub fn put_table<T>(
    req: HttpRequest,
    config: web::Data<Config<T>>,
    body: Option<Json<Value>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
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

    let response = queries::update_table_rows(&config.get_ref().inner, params)
        .map_err(Error::from)
        .and_then(|num_rows_affected| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
        });

    Either::B(response)
}

/// Resets all caches (currently only Table Stats)
pub fn reset_caches<T>(
    config: web::Data<Config<T>>,
) -> impl Future<Item = HttpResponse, Error = Error>
where
    // <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    // <T as MakeTlsConnect<Socket>>::Stream: Send,
    // <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    config
        .get_ref()
        .inner
        .reset_cache()
        .then(|result| match result {
            Ok(_) => Ok(HttpResponseBuilder::new(StatusCode::OK).finish()),
            Err(e) => Err(Error::from(e)),
        })
}
