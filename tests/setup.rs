use actix::System;
use actix_web::{test::block_fn, App, HttpServer};
use experiment00::{db::connect, generate_rest_api_scope, AppConfig};
use futures::{stream::Stream, Future};
use std::{fs::read_to_string, thread::spawn};

pub fn setup_db(db_url: &'static str) {
    let setup_sql_file_path = [env!("CARGO_MANIFEST_DIR"), "tests", "setup_db.sql"].join("/");
    let sql_str = read_to_string(setup_sql_file_path).unwrap();
    let _simple_query_messages = block_fn(|| {
        connect(db_url).and_then(|mut client| {
            client
                .simple_query(&sql_str)
                .collect()
        })
    })
    .unwrap();
}

pub fn start_web_server(db_url: &'static str, address: &'static str) {
    spawn(move || {
        let sys = System::new("experiment00"); // create Actix runtime

        // start 1 server on each cpu thread
        HttpServer::new(move || {
            let mut config = AppConfig::new();
            config.db_url = db_url;

            App::new().service(
                generate_rest_api_scope(config),
            )
        })
        .bind(address)
        .expect("Can not bind to port.")
        .start();

        println!("Running server on {}", address);
        sys.run().unwrap();
    });
}
