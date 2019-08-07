use actix::{spawn as actix_spawn, System};
use actix_web::{test::block_fn, App, HttpServer};
use experiment00::{generate_rest_api_scope, AppConfig};
use futures::{stream::Stream, Future};
use std::{fs::read_to_string, thread::spawn};
use tokio_postgres::{connect, NoTls};

pub fn setup_db(db_url: &'static str) {
    let setup_sql_file_path = [env!("CARGO_MANIFEST_DIR"), "tests", "setup_db.sql"].join("/");
    let sql_str = read_to_string(setup_sql_file_path).unwrap();
    let _simple_query_messages = block_fn(|| {
        connect(db_url, NoTls).and_then(|(mut client, connection)| {
            actix_spawn(connection.map_err(|e| panic!("{}", e)));
            client.simple_query(&sql_str).collect()
        })
    })
    .unwrap();
}

pub fn start_web_server(db_url: &'static str, address: &'static str) {
    let no_cache_port = "8000";
    let cache_port = "8001";

    spawn(move || {
        let sys = System::new("experiment00"); // create Actix runtime

        let address_no_cache = [address, no_cache_port].join(":");
        let address_cache = [address, cache_port].join(":");

        HttpServer::new(move || {
            let mut config = AppConfig::new();
            config.db_url = db_url;
            config.is_custom_sql_execution_endpoint_enabled = true;

            App::new().service(generate_rest_api_scope(config))
        })
        .bind(&address_no_cache)
        .expect("Can not bind to port.")
        .start();

        HttpServer::new(move || {
            let mut config = AppConfig::new();
            config.db_url = db_url;

            App::new().service(generate_rest_api_scope(config))
        })
        .bind(&address_cache)
        .expect("Can not bind to port.")
        .start();

        println!("Running server on {}", &address_no_cache);
        println!("Running server on {}", &address_cache);
        sys.run().unwrap();
    });
}
