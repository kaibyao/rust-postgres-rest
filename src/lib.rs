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
use futures03::compat::Future01CompatExt;
use futures03::future::FutureExt;
use futures03::TryFutureExt;
use futures01::future::{Either, err};
mod queries;

mod compat;

mod db;
use crate::db::{PgConnection, Pool};

mod endpoints;
use endpoints::{get_all_table_names, get_table, index, post_table};

mod errors;

use queries::query_types::{QueryParamsInsert, QueryParamsSelect, RequestQueryStringParams};
use serde_json::Value;

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
                    | {
                        let params = QueryParamsSelect::from_http_request(req, query_string_params.into_inner());
                        get_table(db, params).boxed().compat()
                    }
                ))
                .route(web::post().to_async(
                    |
                        req: HttpRequest,
                        db: web::Data<Pool>,
                        body: web::Json<Value>,
                        query_string_params: web::Query<RequestQueryStringParams>,
                    | {
                        let actual_body = body.into_inner();
                        let params = match QueryParamsInsert::from_http_request(
                            &req,
                            actual_body,
                            query_string_params.into_inner(),
                        ) {
                            Ok(insert_params) => insert_params,
                            Err(e) => return Either::A(err(e)),
                        };

                        Either::B(post_table(db, params).boxed().compat())
                    }
                ))
        )
}
