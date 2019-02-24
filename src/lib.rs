// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

// external crates
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde;

use actix::{Addr, SyncArbiter};
use actix_web::{actix, http::Method, App};

// library modules
mod queries;

mod db;
use crate::db::{init_connection_pool, DbExecutor};

mod endpoints;
use endpoints::{get_all_table_names, index, query_table};

mod errors;

pub struct AppConfig<'a> {
    pub database_url: &'a str,
    pub scope_name: &'a str,
}

pub struct AppState {
    db: Addr<DbExecutor>,
}

/// Takes an initialized App and config, and appends the Rest API functionality to the scope’s endpoint.
pub fn add_rest_api_scope(config: &AppConfig, app: App) -> App {
    // create database connection pool
    let pool = init_connection_pool(config.database_url);

    // create a SyncArbiter (Event Loop Controller) with a DbExecutor actor with worker threads == cpu thread
    let db_addr = SyncArbiter::start(num_cpus::get(), move || DbExecutor(pool.clone()));

    app.scope(config.scope_name, |scope| {
        scope.with_state(
            "",
            AppState {
                db: db_addr.clone(),
            },
            |nested_scope| {
                nested_scope
                    .resource("", |r| r.method(Method::GET).f(index))
                    .resource("/", |r| r.method(Method::GET).f(index))
                    .resource("/table", |r| {
                        r.method(Method::GET).a(get_all_table_names)
                        // POST: create new table
                        // PUT: update table
                        // DELETE: delete table, requires table_name
                    })
                    .resource("/{table}", |r| {
                        r.method(Method::GET).a(query_table)
                        // POST: (bulk) insert
                        // PUT OR PATCH: (bulk) upsert
                        // DELETE: delete rows (also requires confirm_delete query parameter)
                    })
            },
        )
    })
}
