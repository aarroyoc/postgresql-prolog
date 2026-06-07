:- module(scram_test, [run_test/0]).

:- use_module(library(format)).
:- use_module(library(lists)).
:- use_module(library(os)).

:- use_module('postgresql', [connect/6, query/3]).

%% env_or_default(+Name, +Default, -Value)
env_or_default(Name, Default, Value) :-
    (   getenv(Name, V)
    ->  Value = V
    ;   Value = Default ).

%% run_test/0
%
% Boots a connection to the docker-compose Postgres using SCRAM-SHA-256
% and runs a trivial SELECT. Exits 0 on success, 1 on failure, 2 on
% setup error.
run_test :-
    env_or_default("DATABASE_HOST",     "127.0.0.1", Host),
    env_or_default("DATABASE_PORT",     "5433",      PortChars),
    number_chars(Port, PortChars),
    env_or_default("DATABASE_USERNAME", "postgres",  User),
    env_or_default("DATABASE_PASSWORD", "postgres",  Pass),
    env_or_default("DATABASE_DB_NAME",  "postgres",  DB),

    format("[..] connecting to ~s:~d as ~s -> ~s~n", [Host, Port, User, DB]),
    connect(User, Pass, Host, Port, DB, Conn),
    format("[ok] handshake completed (SCRAM-SHA-256)~n", []),

    query(Conn, "SELECT 'scram-sha-256 ok'::text", Result),
    (   Result = data(_Headers, [["scram-sha-256 ok"]])
    ->  format("[OK] scram_test passed~n", []), halt(0)
    ;   format("[KO] unexpected result shape: ~w~n", [Result]), halt(1)
    ).
