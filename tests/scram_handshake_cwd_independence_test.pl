:- module(scram_handshake_cwd_independence_test, [run_test/0]).

:- use_module(library(format)).
:- use_module(library(lists)).
:- use_module(library(os)).

:- use_module('../postgresql', [connect/6, query/3]).

/*
Regression: SCRAM handshake must succeed when scryer-prolog is
invoked with a current working directory other than this package's
own root.

The shipping scram_test.pl runs from the package root, so any
relative-path `use_module(_)` call inside postgresql.pl trivially
resolves -- the package files are right next to CWD. That hides a
real failure mode: consumers (e.g. a downstream project that
vendors postgresql-prolog as a submodule under deps/) run scryer
from the consumer's own root, with CWD != this package. Any
runtime `use_module(Name)` inside postgresql.pl then tries to
open `<consumer-cwd>/Name.pl` and throws

    error(existence_error(source_sink, "scram.pl"), open/4)

on the very first SASL `AuthenticationSASL` message.

This test imports postgresql via the same kind of relative-prefix
path a consumer would use, and the justfile / CI invocation runs
it with `cd /tmp && scryer-prolog ...` so the CWD is *not* the
package root. A green result here means the SCRAM module is
resolved at compile time (when relative paths are anchored to the
calling .pl file), not at runtime (when they are anchored to CWD).
*/

env_or_default(Name, Default, Value) :-
    (   getenv(Name, V)
    ->  Value = V
    ;   Value = Default ).

run_test :-
    env_or_default("DATABASE_HOST",     "127.0.0.1", Host),
    env_or_default("DATABASE_PORT",     "5433",      PortChars),
    number_chars(Port, PortChars),
    env_or_default("DATABASE_USERNAME", "postgres",  User),
    env_or_default("DATABASE_PASSWORD", "postgres",  Pass),
    env_or_default("DATABASE_DB_NAME",  "postgres",  DB),

    format("[..] connecting to ~s:~d as ~s -> ~s (CWD != package root)~n",
           [Host, Port, User, DB]),

    catch(
        connect(User, Pass, Host, Port, DB, Conn),
        E,
        ( format("[KO] connect/6 threw during SCRAM handshake: ~q~n", [E]),
          format("     (this is the runtime use_module(scram) bug)~n", []),
          halt(1) )
    ),

    query(Conn, "SELECT 'scram-sha-256 ok'::text", Result),
    (   Result = data(_Headers, [["scram-sha-256 ok"]])
    ->  format("[OK] SCRAM handshake survives a non-package CWD~n", []),
        halt(0)
    ;   format("[KO] unexpected query result shape: ~w~n", [Result]),
        halt(1)
    ).
