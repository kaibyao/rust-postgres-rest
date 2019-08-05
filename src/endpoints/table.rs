use actix_web::{
    dev::HttpResponseBuilder,
    http::StatusCode,
    web::{self, Json},
    HttpMessage, HttpRequest, HttpResponse,
};
use futures::{
    future::{err, ok, Either},
    Future,
};
use serde_json::{json, Value};

use crate::{
    db::connect,
    queries::{
        delete_table_rows, execute_sql_query, insert_into_table, query_types, select_all_tables,
        select_table_rows, select_table_stats, update_table_rows,
    },
    stats_cache::StatsCacheMessage,
    AppState, Error,
};
use query_types::{
    QueryParamsDelete, QueryParamsExecute, QueryParamsInsert, QueryParamsSelect, QueryParamsUpdate,
    RequestQueryStringParams,
};

/// Deletes table rows and optionally returns the column data in the deleted rows.
pub fn delete_table(
    req: HttpRequest,
    state: web::Data<AppState>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let params = match QueryParamsDelete::from_http_request(&req, query_string_params.into_inner())
    {
        Ok(params) => params,
        Err(e) => return Either::A(err(e)),
    };

    if params.confirm_delete.is_none() {
        return Either::A(err(Error::generate_error(
            "REQUIRED_PARAMETER_MISSING",
            "URL query parameter `confirm_delete` is necessary for table row deletion.".to_string(),
        )));
    }

    let delete_table_future = delete_table_rows(state.get_ref(), params)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(delete_table_future)
}

/// Executes the given SQL statement
pub fn execute_sql(
    req: HttpRequest,
    body: String,
    state: web::Data<AppState>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let content_type = req.content_type().to_lowercase();
    if &content_type != "text/plain" {
        return Either::A(err(Error::generate_error(
            "INVALID_CONTENT_TYPE",
            format!("Content type sent was: `{}`.", content_type),
        )));
    }

    let params = QueryParamsExecute {
        statement: body,
        is_return_rows: query_string_params.is_return_rows.is_some(),
    };

    let execute_sql_future = connect(state.config.db_url)
        .map_err(Error::from)
        .and_then(|client| execute_sql_query(client, params))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)));

    Either::B(execute_sql_future)
}

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    state: web::Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    connect(state.config.db_url)
        .map_err(Error::from)
        .and_then(|client| select_all_tables(client).map_err(Error::from))
        .and_then(|(rows, _client)| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
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

/// Inserts new rows into a table. Returns the number of rows affected.
pub fn post_table(
    req: HttpRequest,
    state: web::Data<AppState>,
    body: Option<Json<Value>>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let actual_body = match body {
        Some(body) => body.into_inner(),
        None => return Either::A(err(Error::generate_error("INCORRECT_REQUEST_BODY", "Request body is required. Body must be a JSON array of objects where each object represents a row and whose key-values represent column names and their values.".to_string())))
    };
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

/// Runs an UPDATE query and returns either rows affected or row columns if specified.
pub fn put_table(
    req: HttpRequest,
    state: web::Data<AppState>,
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

    let params = match QueryParamsUpdate::from_http_request(
        &req,
        actual_body,
        query_string_params.into_inner(),
    ) {
        Ok(params) => params,
        Err(e) => {
            return Either::A(err(e));
        }
    };

    let response = update_table_rows(state.get_ref(), params).and_then(|num_rows_affected| {
        Ok(HttpResponseBuilder::new(StatusCode::OK).json(num_rows_affected))
    });

    Either::B(response)
}

/// Resets all caches (currently only Table Stats)
pub fn reset_caches(state: web::Data<AppState>) -> impl Future<Item = HttpResponse, Error = Error> {
    match &state.get_ref().stats_cache_addr {
        Some(addr) => {
            let reset_cache_future = addr
                .send(StatsCacheMessage::ResetCache)
                .map_err(Error::from)
                .and_then(|response_result| match response_result {
                    Ok(_response_ok) => ok(HttpResponseBuilder::new(StatusCode::OK).finish()),
                    Err(e) => err(e),
                });
            Either::A(reset_cache_future)
        }
        None => Either::B(err(Error::generate_error(
            "TABLE_STATS_CACHE_NOT_INITIALIZED",
            "The cache to be reset was not found.".to_string(),
        ))),
    }
}
