// This file contains a reference implementation of the library using actix-web and r2d2_postgres

#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

use actix::System;
use actix_web::{App, HttpServer};
use experiment00::{generate_rest_api_scope, AppConfig};

fn main() {
    let sys = System::new("experiment00"); // create Actix runtime

    let ip_address = "127.0.0.1:8000";

    // start 1 server on each cpu thread
    HttpServer::new(move || {
        let mut config = AppConfig::new();
        config.db_url = "postgresql://kaiby@localhost:5432/crossroads";
        config.is_cache_table_stats = true;
        config.is_cache_reset_endpoint_enabled = true;
        config.cache_reset_interval_seconds = 300;

        App::new().service(
            // appends an actix-web Scope under the "/api" endpoint to app and returns it
            generate_rest_api_scope(config),
        )
    })
    .bind(ip_address)
    .expect("Can not bind to port 8000")
    .start();

    println!("Running server on {}", ip_address);
    sys.run().unwrap();
}
