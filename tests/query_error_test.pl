:- module(query_error_test, [run_test/0]).

:- use_module(library(format)).
:- use_module(library(lists)).
:- use_module(library(os)).

:- use_module('../postgresql', [connect/6, query/3, query/4]).

env_or_default(Name, Default, Value) :-
    (   getenv(Name, V)
    ->  Value = V
    ;   Value = Default ).

check(Label, Goal) :-
    (   catch(Goal, E, (format("[KO] ~s: threw ~q~n", [Label, E]), fail))
    ->  format("[OK] ~s~n", [Label])
    ;   format("[KO] ~s~n", [Label]),
        halt(1) ).

answers(Connection) :-
    query(Connection, "SELECT 42", [], data([["42"]])).

run_test :-
    env_or_default("DATABASE_HOST",     "127.0.0.1", Host),
    env_or_default("DATABASE_PORT",     "5433",      PortChars),
    number_chars(Port, PortChars),
    env_or_default("DATABASE_USERNAME", "postgres",  User),
    env_or_default("DATABASE_PASSWORD", "postgres",  Pass),
    env_or_default("DATABASE_DB_NAME",  "postgres",  DB),
    atom_chars(HostAtom, Host),
    connect(User, Pass, HostAtom, Port, DB, Connection),

    check("a bind-step error is returned as error(_)",
          query(Connection, "SELECT $1::int", ["not a number"], error(_))),
    check("the connection answers after a bind-step error", answers(Connection)),

    query(Connection, "DROP TABLE IF EXISTS query_error_probe", ok),
    query(Connection, "CREATE TABLE query_error_probe (id int UNIQUE)", ok),
    query(Connection, "INSERT INTO query_error_probe (id) VALUES ($1)", ["1"], _),
    check("an execute-step error is returned as error(_)",
          query(Connection, "INSERT INTO query_error_probe (id) VALUES ($1)", ["1"], error(_))),
    check("the connection answers after an execute-step error", answers(Connection)),

    check("a division by zero is returned as error(_)",
          query(Connection, "SELECT 1/0", [], error(_))),
    check("the connection answers after a division by zero", answers(Connection)),

    query(Connection, "DROP TABLE query_error_probe", ok),
    format("[OK] query_error_test passed~n", []).
