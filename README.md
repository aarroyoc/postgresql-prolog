# postgresql-prolog
A Prolog library to connect to PostgreSQL databases

# Compatible systems

* [Scryer Prolog](https://github.com/mthom/scryer-prolog)

# Installation

The library itself are just four Prolog files (postgresql.pl, sql_query.pl, messages.pl and types.pl). They need to be in the same folder. An easy way to install this library in your project is copying that files. Other way is using Git submodules to get this whole folder, and load the postgresql.pl file from there.

```
git submodule add https://github.com/aarroyoc/postgresql-prolog postgresql
```

# Usage

The library provides four predicates: `connect/6`, `sql/3`, `query/3` and `query/4`.

```
connect(+User, +Password, +Host, +Port, +Database, -Connection)
```
Connects to a PostgreSQL server and tries to authenticate using `password` scheme. This is the only authentication method supported right now. Please, note that this auth method is not the default in some PostgreSQL setups, you changes are needed. If you're running PostgreSQL in Docker, you need to set the environment variable `POSTGRES_HOST_AUTH_METHOD` to `password`.

```
sql(+Connection, +QueryDSL, -Result)
```
Executes a SQL query using the Prolog DSL. This the **recommended** way to do queries as it is the **safest** (it escapes all strings). The DSL right now is limited to a few keywords. Feel free to open a PR to grow the DSL if you need it. The DSL is composed a list of commands, each command may contain different things. The DSL makes some incorrect queries invalid but it isn't a full fledged SQL syntax checker.

Items of the DSL:

* `select(col1, col2, ...)` - equivalent to `SELECT col1, col2, ... . Functions are allowed. For example: `select(sum(visits))` is valid and will generate `SELECT sum(visits)`.
* `from(table)` - equivalent to `FROM table`
* `natural_join(table)` - equivalent to `NATURAL JOIN table`
* `join(table)` / `inner_join(table)` - equivalent to `INNER JOIN table`. Requires a `on` or `using` after.
* `left_join(table)` / `left_outer_join` - equivalent to `LEFT OUTER JOIN table`. Requires a `on` or `using` after.
* `right_join(table)` / `right_outer_join` - equivalent to `RIGHT OUTER JOIN table`. Requires a `on` or `using` after.
* `full_join(table)` / `full_outer_join` - equivalent to `FULL OUTER JOIN table`. Requires a `on` or `using` after.
* `on(Cond)` - equivalent to `ON Cond`. See `where` for details.
* `using(col) - equivalent to `USING (col)`.
* `where(Cond)` - equivalent to `WHERE Cond`. Cond is a Prolog condition. Meaning that you can use parens, AND is (,), OR is (;) and =, \=, >= and =< are used instead of the SQL operators. Strings that appear in the condition are escaped correctly. This is **safe**.
* `group_by(col1)` - equivalent to GROUP BY col1.
* `order_by(kind(col1), kind(col2),...)` - equivalent to ORDER BY col1 KIND, col2 KIND, .... kind must be either `asc` or `desc`.
* `offset(N)` - equivalent to `OFFSET N ROWS`.
* `fetch_first(N, type)` - equivalent to `FETCH FIRST N ROWS type`. Type can be `only (ONLY)` or `with_ties (WITH TIES)`.
* `insert_into(table, [col1, col2,...])` - equivalent to `INSERT INTO table (col1, col2, ...)`. Must be followed by `values`.
* `values(val1, val2, ...)` - equivalent to `VALUES (val1, val2, ...)`. Strings here are escaped so it's **safe**.
* `update(table)` - equivalent to `UPDATE table`. Must be followed by `set`
* `set(Sets)` - equivalent to `SET Col1 = Val1, ...`. Similar to WHERE but only = is allowed. Optionally, you can add a `where+` after a `set`.
* `delete(table)` - equivalent to `DELETE table`. Must be followed by `where`.

Examples:

```
QueryDSL = [select(title), from(posts), where(lang = "es"), order_by(desc(date)), offset(10), fetch_first(5, only)],
SQL = "SELECT title FROM posts WHERE lang = $1 ORDER BY date DESC OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY"

QueryDSL = [select('posts.title','author.name'), from(posts), join(author), on('author.author_id' = 'posts.author_id')]
SQL = "SELECT posts.title,author.name FROM posts INNER JOIN author ON author.author_id = posts.author_id"

QueryDSL = [select(title), from(posts), where((lang = "es";lang='NULL')), order_by(desc(date), asc(title))]
SQL = "SELECT title FROM posts WHERE (lang = $1) OR (lang = NULL) ORDER BY date DESC,title ASC"

QueryDSL = [insert_into(test_table, [name]), values("test")]
SQL = "INSERT INTO test_table (name) VALUES ($1)"

QueryDSL = [update(post),set((lang = "es", price = 99)),where(lang = "fr")]
SQL = "UPDATE post SET lang = $1,price = 99 WHERE lang = $2"

```

The Result var contains the result exection of the query. It can be:

- error(ErrorString)
- data(Rows)

An OK response would be a data response with empty Rows. 


```
query(+Connection, +Query, -Result)
```
Executes a SQL query string over a connection. Result can be:

- ok
- error(ErrorString)
- data(ColumnDescription, Rows) 

ok is returned if the query doesn't output a table (INSERT, UPDATE, DELETE, CREATE TABLE, ...) and succeeds.

error(ErrorString) is returned if an error is found.

data(ColumnDescription, Rows) is returned when a query outputs a table (SELECT). ColumnDescription is a list of column names and Rows is a list of a list of each cell value.

```
query(+Connection, +QueryEscaped, +Data, -Result)
```

Executes a SQL query string over a connection. In contrast to `query/3`, here the query needs to be a template and the vars are passed in the Data list. This is safer than `query/3`. Result is the same as `sql/3`.


# Examples

```
:- use_module('postgresql').

test :-
    postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
    postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
    postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
    postgresql:sql(Connection, [insert_into(test_table, [name]), values("test")], data([])),
    postgresql:sql(Connection, [select(id, name), from(test_table), where(name = "test")], Rows),
    Rows = data([["1", "test"]]),
    postgresql:sql(Connection, [update(test_table), set(name = "test2"), where(id = 1)], data([])),
    postgresql:sql(Connection, [select(id, name), from(test_table), where(name = "test")], Rows2),
    Rows2 = data([]).
```