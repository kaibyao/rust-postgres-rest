# experiment00

Toy project for exploring Rust. Really, I'm trying to see how easy it is to recreate ServiceNow's Table API in Rust using the PostgreSQL database, as I've found that feature to be useful enough to use in future web projects.

## What about PostgREST?

Theoretically I could just use that, but I'm doubting its performance. It’s probably worth benchmarking, but there's a good chance that it wouldn’t be very fast.

1. There's no Haskell web frameworks that perform that well (see [TechEmpower Benchmarks](https://www.techempower.com/benchmarks/#section=data-r17&hw=cl&test=fortune&l=yyku67-1)). PostgREST uses Warp, which is the same web framework that yesod is based on (last place in that list).
1. I'm not interested in learning/working with Haskell.

## Not supported

- Bit, Unknown, and Varbit types are not supported.
- Exclusion and Trigger constraints are not yet supported.

## To dos

1. Recreate the Table API.
1. CSV, XML for REST API
1. MAC Address formatting
1. Replace r2d2 with tokio-postgres (look at techempower benchmarks code)
1. Add security, customizability, optimizations, etc.
1. GraphQL API
1. gRPC, Flatbuffers

## Notes

- Need to be able to query for foreign key values (also need to account for when the user attempts to get the foreign key values for fields that aren't actually foreign keys)
- Dotwalking foreign keys (also see [Resource embedding](http://postgrest.org/en/v5.2/api.html#resource-embedding))
- there should probably be an option for users to add custom API endpoint/configuration for `add_rest_api_scope()`
- Need to add a query parser for all endpoints
- Change String usage to &str for performance reasons
- Convert HashMap to tuples, also for performance reasons

## How API requests should work

- requesting to `/{table}` without `columns`: number of rows (`count(*)`), relations (references and referenced_by), and column names and their type

- requesting with columns but without `where` returns up to 10000 rows, naiively requesting from DB
