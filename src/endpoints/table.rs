use actix::Addr;

use actix_web::dev::HttpResponseBuilder;
use actix_web::http::StatusCode;
use actix_web::web::Json;
use actix_web::{web, HttpRequest, HttpResponse};
use futures::stream::Stream;
use futures::{future, Future};
use tokio_postgres::{Client, Error};

use crate::db::Pool;
use crate::errors::ApiError;
use crate::queries::get_all_tables;
use crate::queries::query_types::{
    Query, QueryParams, QueryParamsInsert, QueryParamsSelect, QueryTasks,
};

/// Retrieves a list of table names that exist in the DB.
pub fn get_all_table_names(
    req: HttpRequest,
    db: web::Data<Pool>,
) -> impl Future<Item = HttpResponse, Error = ApiError> {
    // actors
    // let query: Query = Query {
    //     params: QueryParams::Select(QueryParamsSelect::from_http_request(&req)),
    //     task: QueryTasks::GetAllTables,
    // };

    // db.send(query)
    //     .then(|rows_result| {
    //         match rows_result {
    //             Ok(rows) => HttpResponseBuilder::new(StatusCode::OK).json(rows),
    //             Err(e) => {
    //                 let err = ApiError::from(e);
    //                 match err {
    //                     ApiError::UserError { http_status, .. } => HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err),
    //                     ApiError::InternalError { http_status, .. } => HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err),
    //                 }
    //             },
    //         }
    //     })
    //     .from_err()

    db.run(
        |mut client| {
            client.prepare("SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;")
            .then(|result| match result {
                Ok(statement) => {
                    let f = client.query(&statement, &[])
                        .map(|row| row.get(0))
                        .collect()
                        .then(move |result: Result<Vec<String>, Error>| match result {
                            Ok(rows) => Ok((rows, client)),
                            Err(e) => Err((e, client)),
                        });

                    futures::future::Either::A(f)
                },
                Err(e) => futures::future::Either::B(futures::future::err((e, client)))
            })
            // .map(move |statement| (client, statement))
            // .and_then(|(mut cl, statement)| cl.query(&statement, &[]).collect().join(futures::future::ok(cl)))
            // .map(|(rows, cl)| {
            //     (rows.iter().map(|r| r.get(0)).collect(), cl)
            // })
            // .then(|result: Result<(Vec<String>, Client), (tokio_postgres::Error, Client)>| match result {
            //     Ok((rows, cl)) => Ok((rows, cl)),
            //     Err((e, cl)) => Err((e, cl))
            // })
        })
        .and_then(|rows| {
            Ok(HttpResponseBuilder::new(StatusCode::OK).json(rows))
        })
        .or_else(|e| {
            let err = ApiError::from(e);
            Err(err)
            // match err {
            //     ApiError::UserError { http_status, .. } => Err(HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err)),
            //     ApiError::InternalError { http_status, .. } => Err(HttpResponseBuilder::new(StatusCode::from_u16(http_status).unwrap()).json(err)),
            // }
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
