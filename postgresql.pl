:- module(postgresql, [connect/6, query/3]).

:- use_module(library(lists)).
:- use_module(library(charsio)).
:- use_module(library(sockets)).

:- use_module('messages').
:- use_module('types').

connect(User, Password, Host, Port, Database, postgresql(Stream)) :-
    socket_client_open(Host:Port, Stream, [type(binary)]),
    startup_message(User, Database, BytesStartup),
    put_bytes(Stream, BytesStartup),
    get_bytes(Stream, BytesAuth),
    auth_message(password, BytesAuth),
    password_message(Password, BytesPassword),
    put_bytes(Stream, BytesPassword),
    get_bytes(Stream, BytesOk),
    auth_ok_message(BytesOk),
    flush_bytes(Stream).

flush_bytes(Stream) :-
    get_bytes(Stream, Bytes),
    (
        Bytes = [90|_] ->
        true
    ;   flush_bytes(Stream)
    ).

query(postgresql(Stream), Query, Result) :-
    query_message(Query, BytesQuery),
    put_bytes(Stream, BytesQuery),
    get_bytes(Stream, BytesResponse),
    try_query_response(Stream, BytesResponse, Result).

% after a query message, the following messages can be received
% - CommandComplete
% - RowDescription -> N DataRow
% - EmptyQueryResponse
% - ErrorResponse
% - NoticeResponse
% and then a ReadyForQuery message
try_query_response(Stream, BytesResponse, Result) :-
    command_complete_message(BytesResponse),!,
    Result = ok,
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_query_response(Stream, BytesResponse, Result) :-
    row_description_message(ColumnsDescription, BytesResponse),!,
    % then zero or more data rows
    get_bytes(Stream, BytesData),
    get_data_rows(Stream, ColumnsData, BytesData),
    Result = data(ColumnsDescription, ColumnsData).
    % until we get a command complete message

try_query_response(Stream, BytesResponse, Result) :-
    empty_query_message(BytesResponse),!,
    Result = [],
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_query_response(Stream, BytesResponse, Result) :-
    error_message(Error, BytesResponse),!,
    Result = error(Error),
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_query_response(Stream, BytesResponse, Result) :-
    notice_message(BytesResponse),!,
    get_bytes(Stream, BytesResponse0),
    try_query_response(Stream, BytesResponse0, Result).

get_data_rows(Stream, [], BytesData) :-
    command_complete_message(BytesData),!,
    get_bytes(Stream, BytesData0),
    ready_for_query_message(BytesData0).

get_data_rows(Stream, [Column|Columns], BytesData) :-
    data_row_message(Column, BytesData),!,
    get_bytes(Stream, BytesData0),
    get_data_rows(Stream, Columns, BytesData0).


% https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3

get_bytes(Stream, Bytes) :-
    get_byte(Stream, BType),
    get_byte(Stream, B3),
    get_byte(Stream, B2),
    get_byte(Stream, B1),
    get_byte(Stream, B0),
    int32(Length, [B3, B2, B1, B0]),
    RemainingBytes is Length - 4,
    get_bytes(Stream, RemainingBytes, Bytes0),
    append([BType, B3, B2, B1, B0], Bytes0, Bytes),
    !.

get_bytes(_, 0, []).
get_bytes(Stream, RemainingBytes, [B|Bytes]) :-
    get_byte(Stream, B),
    RemainingBytes1 is RemainingBytes - 1,
    get_bytes(Stream, RemainingBytes1, Bytes).

put_bytes(_, []).
put_bytes(Stream, [Byte|Bytes]) :-
    put_byte(Stream, Byte),
    put_bytes(Stream, Bytes),
    !.