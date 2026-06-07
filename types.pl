:- module(types, [int16/2, int32/2, pstring/2]).

:- use_module(library(lists)).
:- use_module(library(charsio)).

% Message Types
% https://www.postgresql.org/docs/current/protocol-message-types.html

int32(Number, [B3, B2, B1, B0]) :-
    var(Number),
    % Multiplication rather than `<<` because Scryer 0.9.4 returns integers
    % from bitshift expressions whose representation breaks first-argument
    % indexing on the receiving multiclause predicate (auth_method_/3).
    % See tests/indexing_regression.pl for a reproducer; the bug is fixed
    % in Scryer 0.10.0.
    Number is B3 * 16777216 + B2 * 65536 + B1 * 256 + B0.

int32(Number, [B3, B2, B1, B0]) :-
    integer(Number),
    B0 is Number /\ 255,
    B1 is (Number >> 8) /\ 255,
    B2 is (Number >> 16) /\ 255,
    B3 is (Number >> 24) /\ 255.

int16(Number, [B1, B0]) :-
    var(Number),
    Number is B1 * 256 + B0.

int16(Number, [B1, B0]) :-
    integer(Number),
    B0 is Number /\ 255,
    B1 is (Number >> 8) /\ 255.

pstring(String, Bytes) :-
    var(Bytes),
    chars_utf8bytes(String, Bytes0),
    append(Bytes0, [0], Bytes).

pstring(String, Bytes) :-
    var(String),
    append(ByteString, [0], Bytes),
    chars_utf8bytes(String, ByteString).