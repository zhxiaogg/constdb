# ConstDB

Provides a HTTP wrapper for the RocksDB, works in standalone mode only for my homelab server.

## Demo

To startup the constdb:

```bash
cargo run -- --root /tmp/constdb
```

To create a database within constdb:

```bash
curl -XPOST -H'content-type:application/json' -d'{"name": "test"}' http://localhost:3000/api/v1/dbs/
```

To create a table within the database:

```bash
curl -XPOST -H'content-type:application/json' -d'{"name": "persons", "primary_keys": ["last_name", "first_name"]}' http://localhost:3000/api/v1/dbs/test/tables/
```

To insert data into the table:

```bash
curl -XPOST -H'content-type:application/json' -d'{"first_name": "Foo", "last_name": "Bar", "age": 10, "gender": "male"}' http://localhost:3000/api/v1/dbs/test/tables/persons/data/
```

To upsert data into the table:

```bash
curl -XPUT -H'content-type:application/json' -d'{"first_name": "Foo", "last_name": "Bar", "age": 11, "address": ""}' http://localhost:3000/api/v1/dbs/test/tables/persons/data/
```

To query the data:

```bash
curl -XGET 'http://localhost:3000/api/v1/dbs/test/tables/persons/data/?last_name=Bar&first_name=Foo'
```
