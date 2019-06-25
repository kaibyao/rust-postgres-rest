use experiment00::{generate_rest_api_scope, AppConfig};
// use std::env::var_os;
use actix::spawn;
use actix_web::{App, HttpServer};
use futures::{stream::Stream, Future};
use futures03::compat::Future01CompatExt;
use std::{fs::read_to_string, io::Result as IoResult};
use tokio_postgres::{connect, Client, Error as PgError, NoTls, SimpleQueryMessage};

pub async fn connect_db(db_url: &'static str) -> Result<Client, PgError> {
    let (client, connection) = connect(db_url, NoTls).compat().await.unwrap();
    spawn(connection.map_err(|e| panic!("{}", e)));

    Ok(client)
}

pub async fn setup_db(db_url: &'static str) -> IoResult<()> {
    let mut client = connect_db(db_url).await.unwrap();

    let sql_str_result = read_to_string("./tests/setup_db.sql");
    if let Err(e) = sql_str_result {
        return Err(e);
    }
    let sql_str = sql_str_result.unwrap();
    let simple_query_messages = client
        .simple_query(&sql_str)
        .collect()
        .compat()
        .await
        .unwrap();
    for msg in simple_query_messages {
        match msg {
            SimpleQueryMessage::Row(row) => {
                let len = row.len();
                dbg!(len);
                if len > 0 {
                    let mut row_values = vec![];
                    for i in 0..len {
                        row_values.push(row.get(i).unwrap());
                    }
                    println!("{} rows affected: {}", len, row_values.join("\n"))
                }
            }
            SimpleQueryMessage::CommandComplete(num_rows) => {
                println!("Num rows modified: {}.", num_rows)
            }
            SimpleQueryMessage::__NonExhaustive => (),
        };
    }

    drop(client);

    Ok(())
}

pub fn start_server(db_url: &'static str) {
    let ip_address = "127.0.0.1:8000";

    HttpServer::new(move || {
        let mut config = AppConfig::new();
        config.database_url = db_url;

        App::new().service(
            // appends an actix-web Scope under the "/api" endpoint to app and returns it
            generate_rest_api_scope(&config),
        )
    })
    .bind(ip_address)
    .expect("Can not bind to port 8000")
    .start();
}
