// This file contains a reference implementation of the library using actix-web and tokio_postgres. Much of the implementation was refactored from TechEmpower's actix benchmarks (https://github.com/TechEmpower/FrameworkBenchmarks/blob/master/frameworks/Rust/actix/).

extern crate actix_web;
extern crate postgres;

use actix_web::{server, App, HttpRequest, Responder};
use postgres::{Connection, TlsMode};

extern crate experiment00;
use experiment00::{create_postgres_url, DatabaseConfig, table_api_resource};

fn greet(req: &HttpRequest) -> impl Responder {
    let to = req.match_info().get("name").unwrap_or("World");
    format!("Hello {}!", to)
}

fn main() {
    let database_url = create_postgres_url(&DatabaseConfig {
        db_host: "localhost".to_string(),
        db_port: 3306,
        db_user: "kaiby".to_string(),
        db_pass: "".to_string(),
        db_name: "crossroads".to_string(),
    });
    let conn = Connection::connect(database_url.to_string(), TlsMode::None).unwrap();

    server::new(|| {
        App::new()
            .scope("/api", table_api_resource(&conn))
            .resource("/{name}", |r| r.f(greet))
    })
    .bind("127.0.0.1:8000")
    .expect("Can not bind to port 8000")
    .run();
}
