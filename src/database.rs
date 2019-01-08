/// Describes the DB config used to connect with the database.
pub struct DatabaseConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_pass: String,
    pub db_name: String,
}

// Creates a PostgreSQL URL in the format of postgresql://[user[:password]@][netloc][:port][/dbname]
pub fn create_postgres_url(config: &DatabaseConfig) -> String {
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
