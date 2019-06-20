use futures03::compat::Future01CompatExt;
use futures01::stream::Stream;

use tokio_postgres::{Client, Error};

/// Retrieves all user-created table names
pub async fn select_all_tables(
    mut client: Client,
) -> Result<(Vec<String>, Client), (Error, Client)> {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";
    let statement = match client.prepare(statement_str).compat().await {
        Ok(statement) => statement,
        Err(e) => return Err((e, client)),
    };

    let table_names = match client
        .query(&statement, &[])
        .map(|row| row.get(0))
        .collect()
        .compat()
        .await
    {
        Ok(rows) => rows,
        Err(e) => return Err((e, client)),
    };

    Ok((table_names, client))
}
