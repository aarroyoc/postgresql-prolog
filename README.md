# postgresql-prolog
A Prolog library to connect to PostgreSQL databases

# Compatible systems

* [Scryer Prolog](https://github.com/mthom/scryer-prolog)

# Installation

The library itself are just three Prolog files (postgresql.pl, messages.pl and types.pl). They need to be in the same folder. An easy way to install this library in your project is copying that files. Other way is using Git submodules to get this whole folder, and load the postgresql.pl file from there.

```
git submodule add https://github.com/aarroyoc/postgresql-prolog postgresql
```

# Usage

The library provides two predicates: `connect/6` and `query/3`.

```
connect(+User, +Password, +Host, +Port, +Database, -Connection)
```
Connects to a PostgreSQL server and tries to authenticate using `password` scheme. This is the only authentication method supported right now. Please, note that this auth method is not the default in some PostgreSQL setups, you changes are needed. If you're running PostgreSQL in Docker, you need to set the environment variable `POSTGRES_HOST_AUTH_METHOD` to `password`.

```
query(+Connection, +Query, -Result)
```
Executes a SQL query over a connection. Result can be:

- ok
- error(ErrorString)
- data(ColumnDescription, Rows)

ok is returned if the query doesn't output a table (INSERT, UPDATE, DELETE, CREATE TABLE, ...) and succeeds.

error(ErrorString) is returned if an error is found.

data(ColumnDescription, Rows) is returned when a query outputs a table (SELECT). ColumnDescription is a list of column names and Rows is a list of a list of each cell value.

# Examples

```
:- use_module('postgresql').

test :-
    connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
    query(Connection, "DROP TABLE IF EXISTS test_table", ok),
    query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
    query(Connection, "INSERT INTO test_table (name) VALUES ('test')", ok),
    query(Connection, "SELECT * FROM test_table", Rows),
    Rows = data(["id", "name"], [["1", "test"]]),
    query(Connection, "UPDATE test_table SET name = 'test2' WHERE id = 1", ok),
    query(Connection, "SELECT * FROM test_table", Rows2),
    data(["id", "name"], [["1", "test2"]]).
```