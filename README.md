# ConstDB

Provides a HTTP wrapper for the RocksDB, works in standalone mode only for my homelab server.

## Demo

To startup the constdb:
```bash
cargo run -- --root /tmp/constdb
```

To create a database within constdb:
```bash
curl -XPOST -H'content-type:application/json' -d'{"name": "test"}' http://localhost:8000/api/v1/dbs
```

To create a table within the database:
```bash
curl -XPOST -H'content-type:application/json' -d'{"name": "persons", "primary_keys": ["last_name", "first_name"]}' http://localhost:8000/api/v1/dbs/test/tables
```

To insert data into the table:
```bash
curl -XPOST -H'content-type:application/json' -d'{"first_name": "Foo", "last_name": "Bar", "age": 10, "gender": "male"}' http://localhost:8000/api/v1/dbs/test/tables/persons
```

To query the data:
```bash
curl -XGET 'http://localhost:8000/api/v1/dbs/test/tables/persons?last_name=Bar&first_name=Foo'
```

## Contribute

I hope the API is ergonomic and intuitive, so guess it and try it, feedbacks are more than welcomed if you found anything surprising or missing.
