// This file contains a reference implementation of the library using actix-web and r2d2_postgres

#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

use actix::System;
use actix_web::{HttpServer, App};
use experiment00::{add_rest_api_scope, AppConfig};

fn main() {
    let sys = System::new("experiment00"); // create Actix runtime
    let ip_address = "127.0.0.1:8000";

    // start 1 server on each cpu thread
    HttpServer::new(move || {
        let app = App::new();

        let mut config = AppConfig::new();
        config.database_url = "postgresql://kaiby@localhost:5432/crossroads";

        // appends an actix-web Scope under the "/api" endpoint to app and returns it
        add_rest_api_scope(
            &config,
            app,
        )
    })
    .bind(ip_address)
    .expect("Can not bind to port 8000")
    .run();

    println!("Running server on {}", ip_address);

    sys.run();
}
