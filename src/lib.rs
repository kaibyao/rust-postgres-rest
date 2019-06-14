// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

// external crates
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate tokio_postgres;

use actix_http::{Error, HttpService, Request, Response};
use actix_server::{Server, ServerConfig};
use actix_service::{NewService};
use actix_web::{App, FromRequest, HttpRequest, HttpResponse, Scope, web};
use actix_web::dev::{MessageBody, Service};
use futures::{Async, Future, Poll};

// library modules
mod queries;

mod db;
use crate::db::PgConnection;

mod endpoints;
use endpoints::{get_all_table_names, index, /*insert_into_table, query_table*/};

mod errors;

pub struct AppConfig<'a> {
    pub database_url: &'a str,
    pub scope_name: &'a str,
}

impl<'a> Default for AppConfig<'a> {
    fn default() -> Self {
        AppConfig {
            database_url: "",
            scope_name: "/api"
        }
    }
}

impl<'a> AppConfig<'a> {
    pub fn new() -> Self {
        AppConfig::default()
    }
}


// struct AppService {
//     db: PgConnection
// }
// impl Service for AppService {
//     type Error = Error;
//     type Future = Box<Future<Item = HttpResponse, Error = Error>>;
//     type Request = HttpRequest;
//     type Response = HttpResponse;

//     #[inline]
//     fn poll_ready(&mut self) -> Poll<(), Self::Error> {
//         Ok(Async::Ready(()))
//     }

//     fn call(&mut self, req: HttpRequest) -> Self::Future {
//         let path = req.match_info();
//     }
// }

// #[derive(Clone)]
// struct AppFactory;

// impl<'a> NewService for AppFactory {
//     type Config = AppConfig<'a>;
//     type Request = Request;
//     type Response = Response;
//     type Error = Error;
//     type Service = AppService;
//     type InitError = ();
//     type Future = Box<Future<Item = Self::Service, Error = Self::InitError>>;

//     fn new_service(&self, config: &AppConfig) -> Self::Future {
//         // create database connection pool
//         Box::new(PgConnection::connect(config.database_url).map(|db| App {
//             db,
//         }))
//     }
// }

/// Takes an initialized App and config, and appends the Rest API functionality to the scope’s endpoint.
pub fn generate_rest_api_scope(config: &AppConfig) -> Scope {
    web::scope(config.scope_name)
        .data(PgConnection::connect(config.database_url))
        .route("", web::get().to(index))
        .route("/", web::get().to(index))
        .route("/table", web::get().to_async(get_all_table_names))
        .service(
            web::resource("/{table}")
                // .route(web::get().to(query_table))
                // .route(web::post().to(insert_into_table))
        )
}
