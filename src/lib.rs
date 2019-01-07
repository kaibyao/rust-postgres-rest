/// Describes the DB config used to connect with the database.
pub struct DatabaseConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_pass: String,
    pub db_name: String,
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
