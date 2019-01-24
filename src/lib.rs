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

use actix_web::{
    actix::{Addr, SyncArbiter},
    http::Method,
    App, AsyncResponder, FutureResponse, HttpRequest, HttpResponse,
};
use failure::Error;
use futures::future::Future;

// library modules
mod queries;
use crate::queries::query_types::{Query, QueryTasks};

mod db;
use crate::db::{init_connection_pool, DbExecutor};

pub struct AppConfig<'a> {
    pub database_url: &'a str,
    pub scope_name: &'a str,
}

struct AppState {
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

fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
    let query = Query {
        columns: vec![],
        conditions: None,
        limit: None,
        order_by: None,
        table: "information_schema.columns".to_string(),
        task: QueryTasks::GetAllTableColumns,
    };
    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
            Err(_) => Ok(HttpResponse::InternalServerError().into()),
        })
        .responder()
}

fn query_table(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
    let query = Query {
        columns: vec![
            "test_bigint".to_string(),
            "test_bigserial".to_string(),
            "test_bit".to_string(),
            "test_bool".to_string(),
            "test_bytea".to_string(),
            "test_char".to_string(),
            "test_citext".to_string(),
            "test_date".to_string(),
            "test_float8".to_string(),
            "test_hstore".to_string(),
            "test_int".to_string(),
            "test_json".to_string(),
            "test_jsonb".to_string(),
            "test_macaddr".to_string(),
            "test_name".to_string(),
            "test_oid".to_string(),
            "test_real".to_string(),
            "test_serial".to_string(),
            "test_smallint".to_string(),
            "test_smallserial".to_string(),
            "test_text".to_string(),
            "test_time".to_string(),
            "test_timestamp".to_string(),
            "test_timestamptz".to_string(),
            "test_uuid".to_string(),
            "test_varbit".to_string(),
            "test_varchar".to_string(),
        ],
        conditions: None,
        limit: None,
        order_by: None,
        table: "test_fields".to_string(),
        task: QueryTasks::QueryTable,
    };
    req.state()
        .db
        .send(query)
        .from_err()
        .and_then(|res| match res {
            Ok(rows) => Ok(HttpResponse::Ok().json(rows)),
            // Err(_) => Ok(HttpResponse::InternalServerError().into()),
            // TODO: proper error handling
            Err(_) => {
                // let mut response = HttpResponse::InternalServerError();
                // let response2 = response.reason(err_str);
                // let response3 = response2.finish();
                Ok(HttpResponse::InternalServerError().into())
            }
        })
        .responder()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
