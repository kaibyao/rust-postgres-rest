use actix_web::dev::HttpResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{web, HttpRequest, HttpResponse};
use futures::compat::Future01CompatExt;
use futures::future::FutureExt;
use futures::TryFutureExt;
use futures01::Future;
use serde_json::Value;

use crate::db::Pool;
use crate::errors::ApiError;
use crate::queries::{
    insert_into_table, query_types, select_all_tables, select_table_rows, select_table_stats,
};
use query_types::{QueryParamsInsert, QueryParamsSelect, RequestQueryStringParams};

/// Retrieves a list of table names that exist in the DB.
pub async fn get_all_table_names(db: web::Data<Pool>) -> Result<HttpResponse, ApiError> {
    let rows = match db
        .run(|client| select_all_tables(client).boxed().compat())
        .compat()
        .await
    {
        Ok(rows) => rows,
        Err(e) => return Err(ApiError::from(e)),
    };

    Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows))
}

/// Queries a table using SELECT.
pub async fn get_table(
    req: HttpRequest,
    db: web::Data<Pool>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> Result<HttpResponse, ApiError> {
    let params = QueryParamsSelect::from_http_request(req, query_string_params.into_inner());

    if params.columns.is_empty() {
        get_table_stats(db, params.table).await
    } else {
        get_table_rows(db, params).await
    }
}

async fn get_table_rows(
    db: web::Data<Pool>,
    params: QueryParamsSelect,
) -> Result<HttpResponse, ApiError> {
    match db
        .run(|client| select_table_rows(client, params).boxed().compat())
        .compat()
        .await
    {
        Ok(rows) => Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)),
        Err(e) => Err(ApiError::from(e)),
    }
}

async fn get_table_stats(db: web::Data<Pool>, table: String) -> Result<HttpResponse, ApiError> {
    match db
        .run(|client| select_table_stats(client, table).boxed().compat())
        .compat()
        .await
    {
        Ok(rows) => Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)),
        Err(e) => Err(ApiError::from(e)),
    }
}

/// Inserts new rows into a table
pub async fn post_table(
    req: HttpRequest,
    db: web::Data<Pool>,
    body: Json<Value>,
    query_string_params: web::Query<RequestQueryStringParams>,
) -> Result<HttpResponse, ApiError> {
    let actual_body = body.into_inner();
    let params = match QueryParamsInsert::from_http_request(
        &req,
        actual_body,
        query_string_params.into_inner(),
    ) {
        Ok(insert_params) => insert_params,
        Err(e) => return Err(e),
    };

    let insert_query_result = db
        .run(|client| insert_into_table(client, params).boxed().compat())
        .compat()
        .await;

    match insert_query_result {
        Ok(rows_affected_result) => {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows_affected_result))
        }
        Err(e) => Err(ApiError::from(e)),
    }
}
