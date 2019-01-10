// This file contains a reference implementation of the library using actix-web and r2

#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

extern crate actix_web;
use actix_web::{
    actix::{Addr, SyncArbiter, System},
    App,
    Error,
    FutureResponse,
    http,
    HttpRequest,
    HttpResponse,
    Responder,
    server,
};

extern crate num_cpus;

extern crate postgres;
use postgres::{Connection};

extern crate r2d2_postgres;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};

extern crate experiment00;
use experiment00::{
    AppState,
    db::{create_postgres_url, DbConfig, DbExecutor, Pool},
    rest_api_scope
};

// fn greet(req: &HttpRequest) -> impl Responder {
//     let to = req.match_info().get("name").unwrap_or("World");
//     format!("Hello {}!", to)
// }

fn index(req: &HttpRequest<AppState>) -> FutureResponse<HttpResponse, Error> {
    req.state()
        .db
        .send()
}

fn main() {
    let actix_system_actor = System::new("experiment00");

    // init db connection pool
    let database_url = create_postgres_url(&DbConfig {
        db_host: "localhost".to_string(),
        db_port: 3306,
        db_user: "kaiby".to_string(),
        db_pass: "".to_string(),
        db_name: "crossroads".to_string(),
    });
    let manager = PostgresConnectionManager::new(database_url, TlsMode::None).unwrap();
    let pool = Pool::new(manager).unwrap();

    // create a SyncArbiter (Event Loop Controller) with a DbExecutor actor with worker threads == cpu thread
    let db_addr = SyncArbiter::start(
        num_cpus::get(),
        move || DbExecutor(pool.clone())
    );

    // start server
    server::new(move || {
        App::with_state(AppState{db: db_addr.clone()})
            .scope("/api", |scope| {
                scope
                    .resource("", |r| {
                        // GET: get list of tables
                        r.method(http::Method::GET).a(index)
                    })
            })
    })
    .bind("127.0.0.1:8000")
    .expect("Can not bind to port 8000")
    .run();


            // for each thread, use the connection pool handler to open a connection

    actix_system_actor.run();
}
