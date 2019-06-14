use actix::Addr;
use actix_web::{
    HttpRequest, HttpResponse,
};
use actix_web::web::Json;
use actix_web::http::{StatusCode};
use actix_web::dev::HttpResponseBuilder;
use futures::{future, Future};
use serde_json::Value;
use tokio_postgres::Error;

use crate::db::PgConnection;
use crate::errors::ApiError;
use crate::queries::query_types::{
    Query, QueryParams, QueryParamsInsert, QueryParamsSelect, QueryTasks,
};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(req: HttpRequest, db: actix_web::web::Data<Addr<PgConnection>>) -> impl Future<Item = HttpResponse, Error = ApiError> {
    let query: Query = Query {
        params: QueryParams::Select(QueryParamsSelect::from_http_request(&req)),
        task: QueryTasks::GetAllTables,
    };

    db.send(query)
        .then(|rows_result| {
            match rows_result {
                Ok(rows) => HttpResponseBuilder::new(StatusCode::OK).json(rows),
                Err(e) => {
                    let err = ApiError::from(e);
                    match err {
                        ApiError::UserError { http_status, .. } => HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err),
                        ApiError::InternalError { http_status, .. } => HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err),
                    }
                },
            }
        })
        .from_err()
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

// /// Queries a table using SELECT.
// pub fn query_table(req: HttpRequest) -> impl Future<Item = HttpResponse, Error = ApiError> {
//     let params = QueryParamsSelect::from_http_request(req);

//     let task = if params.columns.is_empty() {
//         QueryTasks::QueryTableStats
//     } else {
//         QueryTasks::QueryTable
//     };

//     let query = Query {
//         params: QueryParams::Select(params),
//         task,
//     };

//     req.state()
//         .db
//         .send(query)
//         .from_err()
//         .and_then(|res| match res {
//             Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
//             Err(err) => Err(err),
//         })
//         .responder()
// }
