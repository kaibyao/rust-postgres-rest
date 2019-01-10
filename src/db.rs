use actix_web::{
    actix::{Actor, Handler,SyncContext},
    // Result,
};
use failure::Error;
use r2d2;
use r2d2_postgres;

use crate::queries::{
    get_all_tables,
    Queries,
    Tasks
};

// might not be needed since we can just use r2d2_postgres' `params`
/// Describes the DB config used to connect with the database.
pub struct DbConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_pass: String,
    pub db_name: String,
}

pub type Pool = r2d2::Pool<r2d2_postgres::PostgresConnectionManager>;
pub type Connection = r2d2::PooledConnection<r2d2_postgres::PostgresConnectionManager>;

/// Executes Database actions/queries.
pub struct DbExecutor(pub Pool);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

impl Handler<Queries> for DbExecutor {
    type Result = Result<Vec<String>, Error>;

    fn handle(&mut self, msg: Queries, _: &mut Self::Context) -> Self::Result {
        let conn = self.0.get()?;

        match msg.task {
            Tasks::GetAllTableFields => get_all_tables(&conn)
        }
    }
}

// Creates a PostgreSQL URL in the format of postgresql://[user[:password]@][netloc][:port][/dbname]
// might not be needed since we can just use r2d2_postgres' `params`
pub fn create_postgres_url(config: &DbConfig) -> String {
    let mut database_url = String::from("postgresql://");

    if config.db_user != "" {
        database_url.push_str(&config.db_user);

        if config.db_pass != "" {
            database_url.push_str(&format!(":{}", &config.db_pass))
        }

        database_url.push_str("@");
    }

    database_url.push_str(&format!(
        "{}:{}/{}",
        &config.db_host, &config.db_port, &config.db_name
    ));

    database_url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_postgres_url_correct() {
        let config = DbConfig {
            db_host: "test_host".to_string(),
            db_port: 1234,
            db_user: "test_user".to_string(),
            db_pass: "test_pass".to_string(),
            db_name: "test_db".to_string(),
        };

        assert_eq!(
            create_postgres_url(&config),
            "postgresql://test_user:test_pass@test_host:1234/test_db"
        );
    }
}
