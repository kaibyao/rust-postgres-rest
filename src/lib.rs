// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

// external crates
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate postgres;
#[macro_use]
extern crate serde_derive;

use actix::{Addr, SyncArbiter};
use actix_web::{actix, http::Method, App};

// library modules
mod queries;

mod db;
use crate::db::{init_connection_pool, DbExecutor};

mod endpoints;
use endpoints::{index, query_table};

pub mod errors;

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
                    .resource("", |r| {
                        // GET: get list of tables
                        // TODO: maybe get list of endpoints?
                        r.method(Method::GET).a(index)
                    })
                    .resource("/", |r| {
                        // GET: get list of tables
                        r.method(Method::GET).a(index)
                    })
                    // .resource("/table", |r| {
                    //     // GET: if table_name is given, get column details for table, otherwise give list of tables
                    //     // POST: create new table
                    //     // PUT: update table
                    //     // DELETE: delete table, requires table_name
                    // })
                    .resource("/{table}", |r| {
                        // GET: query table
                        r.method(Method::GET).a(query_table)
                        // POST: (bulk) insert
                        // PUT OR PATCH: (bulk) upsert
                        // DELETE: delete rows (also requires confirm_delete query parameter)
                    })
            },
        )
    })
}
