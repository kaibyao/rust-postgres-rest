use actix_web::dev::HttpResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, web};
use futures03::compat::Future01CompatExt;
use futures03::future::{FutureExt};
use futures03::{TryFutureExt};
use futures01::Future as Future01;

use crate::db::Pool;
use crate::errors::ApiError;
use crate::queries::{
    insert_into_table, query_types, select_all_tables, select_table_rows, select_table_stats,
};
use query_types::{QueryParamsInsert, QueryParamsSelect};

// fn wrap_async_func<F, U, T, Ok, Error>(
//     f: F,
// ) -> impl Fn(U) -> Box<dyn Future01<Item = Ok, Error = Error>> + Clone + 'static
// where
//     Ok: 'static,
//     Error: 'static,
//     F: Fn(U) -> T + Clone + 'static,
//     T: Future3<Output = Result<Ok, Error>> + 'static,
// {
//     move |u| {
//         // Turn a future3 Future into futures1 Future
//         let fut1 = f(u).boxed_local().compat();
//         Box::new(fut1)
//     }
// }

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
    db: web::Data<Pool>,
    params: QueryParamsSelect,
) -> Result<HttpResponse, ApiError> {
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
    db: web::Data<Pool>,
    params: QueryParamsInsert,
) -> Result<HttpResponse, ApiError> {
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
