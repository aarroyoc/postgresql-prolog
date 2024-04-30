:- use_module(postgresql).
:- use_module(sql_query).

:- object(tests, extends(lgtunit)).

    test(trivial) :- true.
    test(password_connection_ok) :- postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", _).
    fails(password_connection_fail) :- postgresql:connect("postgres", "invalid", '127.0.0.1', 5432, "postgres", _).

    test(create_table_insert_and_select) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
        postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
        postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
        postgresql:query(Connection, "INSERT INTO test_table (name) VALUES ('test')", ok),
        postgresql:query(Connection, "SELECT * FROM test_table", Rows),
        Rows = data(["id", "name"], [["1", "test"]]).

    test(table_already_exists) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
        postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
        postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
        postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", error(_)).


    test(update_data) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
        postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
        postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
        postgresql:query(Connection, "INSERT INTO test_table (name) VALUES ('test')", ok),
        postgresql:query(Connection, "SELECT * FROM test_table", Rows),
        Rows = data(["id", "name"], [["1", "test"]]),
        postgresql:query(Connection, "UPDATE test_table SET name = 'test2' WHERE id = 1", ok),
        postgresql:query(Connection, "SELECT * FROM test_table", Rows2),
        Rows2 = data(["id", "name"], [["1", "test2"]]).

    test(extended_query) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
	postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
	postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
	postgresql:query(Connection, "INSERT INTO test_table (name) VALUES ('test')", ok),
	postgresql:query(Connection, "SELECT * FROM test_table WHERE name = $1", ["test"], Rows),
	Rows = data([["1", "test"]]),
	postgresql:query(Connection, "UPDATE test_table SET name = 'test2' WHERE id = 1", ok),
	postgresql:query(Connection, "SELECT * FROM test_table WHERE name = $1", ["test"], Rows2),
	Rows2 = data([]).

    test(sql_query_simple) :-
        sql_query:sql_query([select(title,post), from(posts)], "SELECT title,post FROM posts", []).

    test(sql_query_group_by) :-
        sql_query:sql_query(
	    [select(sum(visits)), from(posts), where(lang = "es"), group_by(title)],
	    "SELECT sum(visits) FROM posts WHERE lang = $1 GROUP BY title",
	    [1-"es"]
	).

    test(sql_query_order_by) :-
        sql_query:sql_query(
	    [select(title,content), from(posts), order_by(desc(date), asc(title))],
	    "SELECT title,content FROM posts ORDER BY date DESC,title ASC",
	    []
	).

    test(sql_query_join) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), join(author), on('author.author_id' = 'posts.author_id')],
	    "SELECT posts.title,author.name FROM posts INNER JOIN author ON author.author_id = posts.author_id",
	    []
	).

    test(sql_query_join_where) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), join(author), on('author.author_id' = 'posts.author_id'), where(lang = "es")],
	    "SELECT posts.title,author.name FROM posts INNER JOIN author ON author.author_id = posts.author_id WHERE lang = $1",
	    [1-"es"]
	).

    test(sql_query_join_using) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), join(author), using(author_id)],
	    "SELECT posts.title,author.name FROM posts INNER JOIN author USING (author_id)",
	    []
	).

    test(full_join) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), full_join(author), using(author_id)],
	    "SELECT posts.title,author.name FROM posts FULL OUTER JOIN author USING (author_id)",
	    []
	).

    test(multiple_join) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), full_join(author), using(author_id), join(comment), on('comment.id' = 5)],
	    "SELECT posts.title,author.name FROM posts FULL OUTER JOIN author USING (author_id) INNER JOIN comment ON comment.id = 5",
	    []
	).

    test(natural_join) :-
        sql_query:sql_query(
	    [select('posts.title','author.name'), from(posts), natural_join(author), where(lang = "es"), order_by(desc(date))],
	    "SELECT posts.title,author.name FROM posts NATURAL JOIN author WHERE lang = $1 ORDER BY date DESC",
	    [1-"es"]
	).

    test(sql_query_or) :-
        sql_query:sql_query(
	    [select(title), from(posts), where((lang = "es";is_null(lang))), order_by(desc(date), asc(title))],
	    "SELECT title FROM posts WHERE (lang = $1) OR (lang IS NULL) ORDER BY date DESC,title ASC",
	    [1-"es"]
	).

    test(fetch_first) :-
        sql_query:sql_query(
	    [select(title), from(posts), where(lang = "es"), order_by(desc(date)), offset(10), fetch_first(5, only)],
	    "SELECT title FROM posts WHERE lang = $1 ORDER BY date DESC OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY",
	    [1-"es"]
	).

    test(insert_into) :-
        sql_query:sql_query(
	    [insert_into(posts, [title, content]), values("Mi nuevo Libro", "Luna de Plutón")],
	    "INSERT INTO posts (title,content) VALUES ($1,$2)",
	    [2-"Luna de Plutón", 1-"Mi nuevo Libro"]
	).

    test(insert_into_on_conflict_do_update) :-
        sql_query:sql_query(
	    [insert_into(posts, [title, content]), values("Mi nuevo Libro", "Luna de Plutón"), on_conflict_do_update(title), set(content='EXCLUDED.content')],
	    "INSERT INTO posts (title,content) VALUES ($1,$2) ON CONFLICT (title) DO UPDATE SET content = EXCLUDED.content",
	    [2-"Luna de Plutón", 1-"Mi nuevo Libro"]
	).

    test(sql_query_update) :-
        sql_query:sql_query(
	    [update(post),set((lang = "es", price = 99))],
	    "UPDATE post SET lang = $1,price = 99",
	    [1-"es"]
	).

    test(sql_query_update) :-
        sql_query:sql_query(
	    [update(post),set((lang = "es", price = 99)),where(lang = "fr")],
	    "UPDATE post SET lang = $1,price = 99 WHERE lang = $2",
	    [2-"fr", 1-"es"]
	).

    test(sql_query_delete) :-
        sql_query:sql_query(
	    [delete(post),where(lang = "fr")],
	    "DELETE FROM post WHERE lang = $1",
	    [1-"fr"]
	).

    test(sql_query) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
	postgresql:query(Connection, "DROP TABLE IF EXISTS test_table", ok),
	postgresql:query(Connection, "CREATE TABLE test_table (id serial, name text)", ok),
	postgresql:sql(Connection, [insert_into(test_table, [name]), values("test")], data([])),
	postgresql:sql(Connection, [select(id, name), from(test_table), where(name = "test")], Rows),
	Rows = data([["1", "test"]]),
	postgresql:sql(Connection, [update(test_table), set(name = "test2"), where(id = 1)], data([])),
	postgresql:sql(Connection, [select(id, name), from(test_table), where(name = "test")], Rows2),
	Rows2 = data([]).

    test(sql_query_2) :-
        postgresql:connect("postgres", "postgres", '127.0.0.1', 5432, "postgres", Connection),
	postgresql:query(Connection, "DROP TABLE IF EXISTS famous", ok),		
	postgresql:query(Connection, "DROP TABLE IF EXISTS country", ok),
	postgresql:query(Connection, "CREATE TABLE country (iso_code varchar(2) PRIMARY KEY, name text)", ok),
	postgresql:query(Connection, "CREATE TABLE famous (id serial PRIMARY KEY, name text, country varchar(2) REFERENCES country(iso_code), year int)", ok),
	postgresql:sql(Connection, [insert_into(country, [iso_code, name]), values("ES", "España")], data([])),
	postgresql:sql(Connection, [insert_into(country, [iso_code, name]), values("PT", "Portugal")], data([])),
	postgresql:sql(Connection, [insert_into(famous, [name, country, year]), values("Miguel de Cervantes", "ES", 1547)], data([])),
	postgresql:sql(Connection, [insert_into(famous, [name, country, year]), values("Magallanes", "PT", 1480)], data([])),
	postgresql:sql(Connection, [insert_into(famous, [name, country, year]), values("Picasso", "ES", 1881), returning(name, country)], data([["Picasso", "ES"]])),
	postgresql:sql(Connection, [select('famous.name','country.name'),from(famous),join(country),on('famous.country' = 'country.iso_code'),where((year > 1500,year < 2000)),order_by(asc(year))], Result),
	Result = data([["Miguel de Cervantes", "España"], ["Picasso", "España"]]).


:- end_object.
