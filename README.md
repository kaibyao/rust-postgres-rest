# experiment00

Toy project for exploring Rust. Really, I'm trying to see how easy it is to recreate ServiceNow's Table API in Rust using the PostgreSQL database, as I've found that feature to be useful enough to use in future web projects.

## Not supported

- Bit, Unknown, and Varbit types are not supported.

## To dos

1. Recreate the Table API.
1. Add security, customizability, optimizations, etc.
1. ???
1. Profit!

## Notes

- Need to add filter conditions (check postgrest as their DSL is actually pretty spot on)
- Need to be able to query for foreign key values (also need to account for when the user attempts to get the foreign key values for fields that aren't actually foreign keys)
- Need to add a query parser for all endpoints
- Change String usage to &str for performance reasons
- Dotwalking foreign keys (also see [Resource embedding](http://postgrest.org/en/v5.2/api.html#resource-embedding))

## How API requests should work

- requesting to `/{table}` without `columns`: number of rows (`count(*)`), relations (references and referenced_by), and column names and their type

- requesting with columns but without `where` returns up to 10000 rows, naiively requesting from DB
