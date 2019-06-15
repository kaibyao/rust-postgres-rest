use actix_web::dev::HttpResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{web, HttpRequest, HttpResponse};
use futures::future::Either;
use futures::stream::Stream;
use futures::Future;
use tokio_postgres::{Client, Error};

use crate::db::Pool;
use crate::errors::ApiError;
use crate::queries::{select_all_tables, /*select_table_rows, */select_table_stats};
use crate::queries::query_types::{
    Query, QueryParams, QueryParamsInsert, QueryParamsSelect, QueryTasks,
};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    db: web::Data<Pool>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    db.run(select_all_tables)
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
        .or_else(|e| {
            let err = ApiError::from(e);
            Err(err)
        })
}

// /// Inserts new rows into a table
// pub fn insert_into_table(
//     (req, state): (HttpRequest, State),
// ) -> impl Future<Item = HttpResponse, Error = ApiError> {
//     Json::<Value>::extract(&req).from_err().and_then(
//         move |body| {
//             let actual_body = body.into_inner();
//             let params = match QueryParamsInsert::from_http_request(&req, actual_body) {
//                 Ok(insert_params) => insert_params,
//                 Err(e) => {
//                     return Box::from(future::err(e));
//                 }
//             };

//             let query: Query = Query {
//                 params: QueryParams::Insert(params),
//                 task: QueryTasks::InsertIntoTable,
//             };

//             state
//                 .db
//                 .send(query)
//                 .from_err()
//                 .and_then(|res| match res {
//                     Ok(num_rows_affected) => Ok(HttpResponse::Ok().json(num_rows_affected)),
//                     Err(err) => Err(err),
//                 })
//                 .responder()
//         },
//     )
// }

/// Queries a table using SELECT.
pub fn query_table(req: HttpRequest, db: web::Data<Pool>) -> impl Future<Item = HttpResponse, Error = ApiError> {
    let params = QueryParamsSelect::from_http_request(&req);

    // if params.columns.is_empty() {
    //     Either::A(get_table_stats(db, params.table))
    // } else {
    //     Either::B(get_table_rows(db, params))
    // }
    get_table_stats(db, params.table)
}

// fn get_table_rows(db: web::Data<Pool>, params: QueryParamsSelect) -> impl Future<Item = HttpResponse, Error = ApiError> {
//     db.run(|client| select_table_rows(client, params))
//         .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
//         .or_else(|e| {
//             let err = ApiError::from(e);
//             Err(err)
//         })
// }

fn get_table_stats(db: web::Data<Pool>, table: String) -> impl Future<Item = HttpResponse, Error = ApiError> {
    db.run(|client| select_table_stats(client, table))
        .and_then(|rows| Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows)))
        .or_else(|e| {
            let err = ApiError::from(e);
            Err(err)
        })
}
