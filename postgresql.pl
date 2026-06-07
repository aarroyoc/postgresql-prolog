:- module(postgresql, [connect/6, query/3, query/4, sql/3]).

:- use_module(library(lists)).
:- use_module(library(charsio)).
:- use_module(library(sockets)).
:- use_module(library(reif)).


:- use_module(messages).
% scram.pl pulls in library(crypto), whose eager load makes the Logtalk
% test suite hang under Scryer 0.9.4 + Logtalk 3.70.0 (the pair this repo
% pins for that job, since Scryer >= 0.10 dropped the Logtalk adapter).
% Load scram lazily, but anchor its path at compile time so the resolution
% does not depend on the consumer's runtime CWD (a regression we hit when
% this package is vendored as a submodule -- see
% tests/scram_handshake_cwd_independence_test.pl).
:- use_module(sql_query).
:- use_module(types).

:- dynamic(scram_module_path/1).

capture_scram_module_path :-
    catch(
        ( prolog_load_context(directory, Dir),
          atom_concat(Dir, '/scram.pl', Path),
          assertz(scram_module_path(Path)) ),
        _,
        true
    ).
:- initialization(capture_scram_module_path).

connect(User, Password, Host, Port, Database, postgresql(Stream)) :-
    (   atom(Host)
    ->  HostAtom = Host
    ;   atom_chars(HostAtom, Host) ),
    socket_client_open(HostAtom:Port, Stream, [type(binary)]),
    startup_message(User, Database, BytesStartup),
    put_bytes(Stream, BytesStartup),
    do_authenticate(Stream, User, Password),
    flush_bytes(Stream).

% Dispatch on the server's AuthenticationRequest method.
do_authenticate(Stream, User, Password) :-
    get_bytes(Stream, BytesAuth),
    auth_method(BytesAuth, Method),
    handle_auth(Method, Stream, User, Password).

handle_auth(ok, _, _, _).
handle_auth(password, Stream, _, Password) :-
    password_message(Password, BytesPassword),
    put_bytes(Stream, BytesPassword),
    get_bytes(Stream, BytesOk),
    auth_ok_message(BytesOk).
handle_auth(sasl(Mechanisms), Stream, User, Password) :-
    scram_module_path(Path),
    use_module(Path, [do_scram_sha_256_after_offer/3]),
    if_(memberd_t("SCRAM-SHA-256", Mechanisms),
        ( scram:do_scram_sha_256_after_offer(Stream, User, Password),
          get_bytes(Stream, BytesOk),
          auth_ok_message(BytesOk)
        ),
        throw(unsupported_sasl_mechanisms(Mechanisms))
    ).

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

% Extended Query. Safer
query(postgresql(Stream), Query, Params, Result) :-
    length(Params, NumberParams),
    parse_message(Query, NumberParams, QueryBytes),
    put_bytes(Stream, QueryBytes),
    flush_message(FlushBytes),
    put_bytes(Stream, FlushBytes),
    get_bytes(Stream, ResponseBytes),
    try_parse_response(Stream, ResponseBytes, Params, Result).

try_parse_response(Stream, BytesData, Params, Result) :-
    parse_complete_message(BytesData),!,
    bind_message(Params, BindBytes),
    put_bytes(Stream, BindBytes),
    flush_message(FlushBytes),
    put_bytes(Stream, FlushBytes),    
    get_bytes(Stream, ResponseBytes),
    try_bind_response(Stream, ResponseBytes, Result).

try_parse_response(Stream, BytesResponse, _, Result) :-
    error_message(Error, BytesResponse),!,
    Result = error(Error),
    sync_message(SyncBytes),
    put_bytes(Stream, SyncBytes),    
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_bind_response(Stream, BytesData, Result) :-
    bind_complete_message(BytesData),!,
    execute_message(ExecuteBytes),
    put_bytes(Stream, ExecuteBytes),
    flush_message(FlushBytes),
    put_bytes(Stream, FlushBytes),    
    get_bytes(Stream, ResponseBytes),
    try_execute_response(Stream, ResponseBytes, Result).
    
try_bind_response(Stream, BytesResponse, _, Result) :-
    error_message(Error, BytesResponse),!,
    Result = error(Error),
    sync_message(SyncBytes),
    put_bytes(Stream, SyncBytes),    
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_execute_response(Stream, BytesResponse, Result) :-
    ext_get_data_rows(Stream, ColumnsData, BytesResponse),
    Result = data(ColumnsData).

try_execute_response(Stream, BytesResponse, Result) :-
    empty_query_message(BytesResponse),!,
    Result = [],
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

try_execute_response(Stream, BytesResponse, Result) :-
    error_message(Error, BytesResponse),!,
    Result = error(Error),
    get_bytes(Stream, BytesEnd),
    ready_for_query_message(BytesEnd).

ext_get_data_rows(Stream, [], BytesData) :-
    command_complete_message(BytesData),!,
    sync_message(SyncBytes),
    put_bytes(Stream, SyncBytes),    
    get_bytes(Stream, BytesData0),
    ready_for_query_message(BytesData0).

ext_get_data_rows(Stream, [Column|Columns], BytesData) :-
    data_row_message(Column, BytesData),!,
    get_bytes(Stream, BytesData0),
    ext_get_data_rows(Stream, Columns, BytesData0).


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

sql(Connection, Query, Result) :-
    sql_query(Query, TextQuery, Vars),
    keysort(Vars, SortedVars),
    maplist(pair_value, SortedVars, QueryVars),
    query(Connection, TextQuery, QueryVars, Result).

pair_value(_-B, B).
