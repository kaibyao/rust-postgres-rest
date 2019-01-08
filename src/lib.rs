extern crate actix_web;
extern crate postgres;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

use crate::actix_web::{http::Method, Scope};
use crate::postgres::{Connection, TlsMode};

mod database;
use crate::database::create_postgres_url;
pub use crate::database::DatabaseConfig;

mod table_api;
use crate::table_api::*;

pub fn table_api_resource<S: 'static>(config: &DatabaseConfig) -> impl Fn(Scope<S>) -> Scope<S> {
    let database_url = create_postgres_url(config);
    let conn = Connection::connect(database_url.to_string(), TlsMode::None).unwrap();

    prepare_all_statements(&conn);

    |scope| {
        scope
            .resource("", |r| {
                // GET: get list of tables
                r.method(Method::GET).f(|_req| match get_all_tables(&conn) {
                    Ok(tables) => json!(tables),
                    Err(message) => json!(ApiError {
                        message: message.to_string()
                    }),
                })
            })
            .resource("/{table}", |r| {
                // GET: query table
                // POST: (bulk) insert
                // PUT OR PATCH: (bulk) upsert
                // DELETE: delete rows (also requires confirm_delete query parameter)
            })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
