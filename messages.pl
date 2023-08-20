:- module(messages, [
    startup_message/3,
    auth_message/2,
    password_message/2,
    auth_ok_message/1,
    query_message/2,
    notice_message/1,
    error_message/2,
    command_complete_message/1,
    empty_query_message/1,
    row_description_message/2,
    data_row_message/2,
    ready_for_query_message/1,
    parse_message/3,
    parse_complete_message/1,
    bind_message/2,
    bind_complete_message/1,
    execute_message/1,
    close_message/1,
    sync_message/1,
    flush_message/1
]).

:- use_module(library(lists)).
:- use_module(library(charsio)).
:- use_module(library(dcgs)).
:- use_module(library(format)).

:- use_module('types').

% Message Formats
% https://www.postgresql.org/docs/current/protocol-message-formats.html

% StartupMessage
startup_message(User, Database, Bytes) :-
    int32(196608, B1), % Version
    pstring("user", B2),
    pstring(User, B3),
    pstring("database", B4),
    pstring(Database, B5),
    phrase((B1, B2, B3, B4, B5, [0]), Bs),
    length(Bs, L),
    BytesLength is L + 4,
    int32(BytesLength, B0),
    append(B0, Bs, Bytes).

% AuthenticationMD5Password
auth_message(md5, Salt, Bytes) :-
    Bytes = [82,_,_,_,_|Bytes0], % Byte R
    Bytes0 = [B7, B6, B5, B4, B3, B2, B1, B0],
    int32(5, [B7, B6, B5, B4]),
    Salt = [B3, B2, B1, B0].

% AuthenticationCleartextPassword
auth_message(password, Bytes) :-
    Bytes = [82,_,_,_,_|Bytes0], % Byte R
    Bytes0 = [B3, B2, B1, B0],
    int32(3, [B3, B2, B1, B0]).

% PasswordMessage
password_message(Password, Bytes) :-
    pstring(Password, Bytes0),
    length(Bytes0, L),
    RealLength is L + 4,
    int32(RealLength, Bytes1),
    append([112|Bytes1], Bytes0, Bytes).

% AuthenticationOk
auth_ok_message(Bytes) :-
    Bytes = [82,0,0,0,8,0,0,0,0]. % Byte R

% Query
query_message(Query, Bytes) :-
    pstring(Query, Bytes0),
    length(Bytes0, L),
    RealLength is L + 4,
    int32(RealLength, Bytes1),
    append([81|Bytes1], Bytes0, Bytes).

% Parse
parse_message(Query, NumberParams, Bytes) :-
    pstring("", Bytes1),
    pstring(Query, Bytes2),
    int16(NumberParams, Bytes3),
    findall(B, (length(X, NumberParams), member(N, X), N = 0, int32(N, B)), Bytes4),
    append(Bytes4, FBytes4),
    append([Bytes1, Bytes2, Bytes3, FBytes4], PreBytes),
    length(PreBytes, L),
    RealLength is L + 4,
    int32(RealLength, Bytes0),
    append([80|Bytes0], PreBytes, Bytes). % Byte P

% ParseComplete
parse_complete_message([49,_,_,_,_]).

% Bind
bind_message(Params, Bytes) :-
    pstring("", Bytes1),
    pstring("", Bytes2),
    int16(0, Bytes3),
    length(Params, NumberParams),
    int16(NumberParams, Bytes4),
    bind_message_params_bytes(Params, [], Bytes5),
    append([Bytes1, Bytes2, Bytes3, Bytes4, Bytes5, Bytes3], PreBytes),
    length(PreBytes, L),
    RealLength is L + 4,
    int32(RealLength, Bytes0),
    append([66|Bytes0], PreBytes, Bytes). % Byte B

bind_message_params_bytes([], B, B).
bind_message_params_bytes([null|Params], Bytes0, Bytes) :-
    int32(-1, B),
    append(Bytes0, B, Bytes1),
    bind_message_params_bytes(Params, Bytes1, Bytes).

bind_message_params_bytes([Param|Params], Bytes0, Bytes) :-
    chars_utf8bytes(Param, B2),
    length(B2, L),
    int32(L, B1),
     append([B1, B2], B),
    append(Bytes0, B, Bytes1),
    bind_message_params_bytes(Params, Bytes1, Bytes).

% BindComplete
bind_complete_message([50,_,_,_,_]).

% Execute
execute_message(Bytes) :-
    int32(9, B1),
    pstring("", B2),
    int32(0, B3),
    append([[69|B1], B2, B3], Bytes).

% Close
close_message(Bytes) :-
    int32(6, B1),
    B2 = [83],
    pstring("", B3),
    append([[67|B1], B2, B3], Bytes).

% Sync
sync_message([83|B]) :-
    int32(4, B).

% Flush
flush_message([72|B]) :-
    int32(4, B).

% ErrorResponse
error_message(Error, Bytes) :-
    Bytes = [69,_,_,_,_,B0|Bytes0], % Byte E
    (B0 = 0 ->
        Error = "No error message"
    ;   pstring(Error, Bytes0)
    ).

% NoticeResponse
notice_message(Bytes) :-
    Bytes = [78|_]. % Byte N

% EmptyQueryResponse
empty_query_message(Bytes) :-
    Bytes = [73,_,_,_,_]. % Byte I

% CommandComplete
command_complete_message(Bytes) :- 
    Bytes = [67|_]. % Byte C

% RowDescription
row_description_message(Columns, Bytes) :-
    Bytes = [84,_,_,_,_|Bytes0], % Byte T
    Bytes0 = [B1, B0|Bytes1],
    int16(Fields, [B1, B0]),
    get_row_fields(Columns, Fields, Bytes1).

split(Bytes, Separator, Bytes0, Bytes1) :-
    append(Bytes0, [Separator|Bytes1], Bytes).

get_row_fields([], 0, _).
get_row_fields([Column|Columns], Fields, Bytes) :-
    split(Bytes, 0, StringBytes, R),
    pstring(Column, StringBytes),
    R = [_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_|NewBytes],
    Fields0 is Fields - 1,
    get_row_fields(Columns, Fields0, NewBytes).

% DataRow
data_row_message(Columns, Bytes) :-
    Bytes = [68,_,_,_,_,B1,B0|Bytes0], % Byte D
    int16(Fields, [B1, B0]),
    get_fields(Columns, Fields, Bytes0).

get_fields([], 0, _).
get_fields([Column|Columns], Fields, Bytes) :-
    Bytes = [B3, B2, B1, B0|Bytes0],
    int32(Length, [B3, B2, B1, B0]),
    Length = 4294967295, % -1
    Column = null,
    Fields0 is Fields - 1,
    get_fields(Columns, Fields0, Bytes0).

get_fields([Column|Columns], Fields, Bytes) :-
    Bytes = [B3, B2, B1, B0|Bytes0],
    int32(Length, [B3, B2, B1, B0]),
    Length \= 4294967295, % -1
    take(Length, Bytes0, ColumnBytes, NewBytes),
    chars_utf8bytes(Column, ColumnBytes),
    Fields0 is Fields - 1,
    get_fields(Columns, Fields0, NewBytes).

take(N, Bytes, Bytes0, Bytes1) :-
    append(Bytes0, Bytes1, Bytes),
    length(Bytes0, N).

% ReadyForQuery
ready_for_query_message(Bytes) :-
    Bytes = [90|_]. % Byte Z
