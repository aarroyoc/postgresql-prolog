:- use_module('postgresql').

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



:- end_object.