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
    // Scope,
    // State,
};
// use failure::Error;
// use futures::future::Future;
// use crate::postgres::{Connection as PgConnection};

// library modules
pub mod queries;
// use crate::queries::{Queries, Tasks};
// use crate::rest_api::*;

pub mod db;
use crate::db::{DbExecutor};

pub struct AppState {
    pub db: Addr<DbExecutor>,
}

// pub fn rest_api_scope<S: 'static>(scope: Scope<S>) -> Scope<S> {
//     // prepare_all_statements(conn);
//     scope
//         .resource("", |r| {
//             // GET: get list of tables
//             r.method(Method::GET).a(index)
//         })
//         // .resource("/{table}", |r| {
//         //     // GET: query table
//         //     // POST: (bulk) insert
//         //     // PUT OR PATCH: (bulk) upsert
//         //     // DELETE: delete rows (also requires confirm_delete query parameter)
//         // })
// }

// fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
//     let query = Queries {
//         limit: 0,
//         task: Tasks::GetAllTableFields
//     };
//     let result = Box::new(req.state()
//         .db
//         .send(query));
// }

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
