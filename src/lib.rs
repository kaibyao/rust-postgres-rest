// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

// external crates
extern crate actix_web;
extern crate failure;
extern crate futures;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
// #[macro_use]
// extern crate serde_derive;
// #[macro_use]
// extern crate serde_json;
use actix_web::{
    actix::{Addr},
    // Error,
    // http::{Method/*, StatusCode*/},
    // HttpRequest,
    // HttpResponse,
    // FutureResponse,
    Scope,
    // State,
};
// use futures::future::Future;
// use crate::postgres::{Connection as PgConnection};

// library modules
mod queries;
// use crate::rest_api::*;

pub mod db;
use crate::db::{DbExecutor};

pub struct AppState {
    pub db: Addr<DbExecutor>,
}

pub fn rest_api_scope<S: 'static>(scope: Scope<S>) -> Scope<S> {
    // prepare_all_statements(conn);
    scope
    // scope
    //     .resource("", |r| {
    //         // GET: get list of tables
    //         r.method(Method::GET).a(index)
    //     })
        // .resource("/{table}", |r| {
        //     // GET: query table
        //     // POST: (bulk) insert
        //     // PUT OR PATCH: (bulk) upsert
        //     // DELETE: delete rows (also requires confirm_delete query parameter)
        // })
}

// fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
//     let mut response = HttpResponse::build_from(req);
//     match get_all_tables(conn) {
//         Ok(tables) =>
//             response
//                 .status(StatusCode::from_u16(200).unwrap())
//                 .json(tables),
//         Err(message) =>
//             response
//                 .status(StatusCode::from_u16(500).unwrap())
//                 .json(
//                     ApiError {
//                         message: message.to_string()
//                     }
//                 ),
//     }
// }

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
