// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

// external crates
extern crate actix_web;
extern crate postgres;
#[macro_use]
extern crate serde_derive;
// #[macro_use]
// extern crate serde_json;
use crate::actix_web::{http::{Method, StatusCode}, HttpResponse, Scope};
use crate::postgres::{Connection};

// library modules
mod table_api;
use crate::table_api::*;

mod database;
pub use crate::database::{create_postgres_url, DatabaseConfig};

pub fn table_api_resource<S: 'static>(conn: &'static Connection) -> impl Fn(Scope<S>) -> Scope<S> {
    prepare_all_statements(conn);

    move |scope| {
        scope
            .resource("", move |r| {
                // GET: get list of tables
                r.method(Method::GET).f(move |req| {
                    let mut response = HttpResponse::build_from(req);
                    match get_all_tables(conn) {
                        Ok(tables) =>
                            response
                                .status(StatusCode::from_u16(200).unwrap())
                                .json(tables),
                        Err(message) =>
                            response
                                .status(StatusCode::from_u16(500).unwrap())
                                .json(
                                    ApiError {
                                        message: message.to_string()
                                    }
                                ),
                    }
                })
            })
            .resource("/{table}", |r| {
                // GET: query table
                // POST: (bulk) insert
                // PUT OR PATCH: (bulk) upsert
                // DELETE: delete rows (also requires confirm_delete query parameter)
            })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
