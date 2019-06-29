// nightly features
#![feature(async_await)]
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
mod queries;

pub mod db;

mod endpoints;
use endpoints::{get_all_table_names, get_table, index, post_table};

mod error;
pub use error::Error;

use actix_web::{web, Scope};

pub struct AppConfig<'a> {
    pub db_url: &'a str,
    pub scope_name: &'a str,
}

impl<'a> Default for AppConfig<'a> {
    fn default() -> Self {
        AppConfig {
            db_url: "",
            scope_name: "/api",
        }
    }
}

impl<'a> AppConfig<'a> {
    pub fn new() -> Self {
        AppConfig::default()
    }
}

/// Takes an initialized App and config, and appends the Rest API functionality to the scope’s
/// endpoint.
pub fn generate_rest_api_scope(config: AppConfig<'static>) -> Scope {
    web::scope(config.scope_name)
        .data(config)
        .route("", web::get().to(index))
        .route("/", web::get().to(index))
        .route("/table", web::get().to_async(get_all_table_names))
        .service(
            web::resource("/{table}")
                .route(web::get().to_async(get_table))
                .route(web::post().to_async(post_table)),
        )
}
