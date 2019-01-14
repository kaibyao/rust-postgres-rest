// This file contains a reference implementation of the library using actix-web and r2d2_postgres

#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]

extern crate actix_web;
use actix_web::{actix::System, server, App};

extern crate experiment00;
use experiment00::{add_rest_api_scope, AppConfig};

fn main() {
    let actix_system_actor = System::new("experiment00");

    // start server
    server::new(|| {
        let app = App::new();

        // appends an actix-web Scope under the "/api" endpoint to app and returns it
        add_rest_api_scope(
            &AppConfig {
                database_url: "postgresql://kaiby@localhost:5432/crossroads",
                scope_name: "/api",
            },
            app,
        )
    })
    .bind("127.0.0.1:8000")
    .expect("Can not bind to port 8000")
    .run();

    actix_system_actor.run();
}
