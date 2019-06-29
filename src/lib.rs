// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

mod db;
mod endpoints;
mod error;
mod queries;

use actix_web::{web, Scope};
use endpoints::{get_all_table_names, get_table, index, post_table};
pub use error::Error;

/// API Configuration
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

/// Creates and returns an actix scope containing the REST API endpoints.
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
