use actix::spawn as actix_spawn;
use actix_web::{test::block_fn, App, HttpServer};
use futures::{stream::Stream, Future};
use rust_postgres_rest_actix::Config;
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
        let address_no_cache = [address, no_cache_port].join(":");

        HttpServer::new(move || {
            App::new().service(
                Config::new(db_url)
                    .enable_custom_sql_url()
                    .generate_scope("/api"),
            )
        })
        .bind(&address_no_cache)
        .expect("Can not bind to port.")
        .run()
        .unwrap();

        println!("Running server on {}", &address_no_cache);
    });

    spawn(move || {
        let address_cache = [address, cache_port].join(":");

        HttpServer::new(move || {
            App::new().service(
                Config::new(db_url)
                    .enable_custom_sql_url()
                    .generate_scope("/api"),
            )
        })
        .bind(&address_cache)
        .expect("Can not bind to port.")
        .run()
        .unwrap();

        println!("Running server on {}", &address_cache);
    });
}
