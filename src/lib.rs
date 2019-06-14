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

use actix_web::{Scope, web};

// library modules
mod queries;

mod db;
use crate::db::PgConnection;

mod endpoints;
use endpoints::{get_all_table_names, index/*, insert_into_table*/, query_table};

mod errors;

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
        .route("/table", web::get().to_async(get_all_table_names))
        .service(
            web::resource("/{table}")
                .route(web::get().to_async(query_table))
                                       // .route(web::post().to(insert_into_table))
        )
}
