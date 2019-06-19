// async/await
#![feature(async_await, futures_api)]
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

// library modules
use actix_web::{web, HttpRequest, Scope};
// use actix_web_async_await::{compat};
use futures::compat::Future01CompatExt;
use futures::future::FutureExt;
use futures::TryFutureExt;
mod queries;

mod db;
use crate::db::{PgConnection, Pool};

mod endpoints;
use endpoints::{get_all_table_names, get_table, index, post_table};

mod errors;

use queries::query_types::{RequestQueryStringParams};

pub struct AppConfig<'a> {
    pub database_url: &'a str,
    pub scope_name: &'a str,
}

impl<'a> Default for AppConfig<'a> {
    fn default() -> Self {
        AppConfig {
            database_url: "",
            scope_name: "/api",
        }
    }
}

impl<'a> AppConfig<'a> {
    pub fn new() -> Self {
        AppConfig::default()
    }
}

/// Takes an initialized App and config, and appends the Rest API functionality to the scope’s endpoint.
pub fn generate_rest_api_scope(config: &AppConfig) -> Scope {
    let pool = PgConnection::connect(config.database_url).unwrap();

    web::scope(config.scope_name)
        .data(pool)
        .route("", web::get().to(index))
        .route("/", web::get().to(index))
        .route("/table", web::get().to_async(|db: web::Data<Pool>| get_all_table_names(db).boxed().compat()))
        .service(
            web::resource("/{table}")
                .route(web::get().to_async(
                    |
                        req: HttpRequest,
                        db: web::Data<Pool>,
                        query_string_params: web::Query<RequestQueryStringParams>
                    |
                    get_table(req, db, query_string_params).boxed().compat()
                ))
                .route(web::post().to_async(post_table))
        )
}
