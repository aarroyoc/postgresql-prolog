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
    ready_for_query_message/1
]).

:- use_module(library(lists)).
:- use_module(library(charsio)).

:- use_module('types').

% Message Formats
% https://www.postgresql.org/docs/current/protocol-message-formats.html

% StartupMessage
startup_message(User, Database, Bytes) :-
    int32(196608, B1),
    pstring("user", B2),
    pstring(User, B3),
    pstring("database", B4),
    pstring(Database, B5),
    append(B1, B2, B12),
    append(B3, B4, B34),
    append(B12, B34, B1234),
    append(B1234, B5, B12345),
    append(B12345, [0], B),
    length(B, L),
    BytesLength is L + 4,
    int32(BytesLength, B0),
    append(B0, B, Bytes).

% AuthenticationMD5Password
auth_message(md5, Salt, Bytes) :-
    Bytes = [82,_,_,_,_|Bytes0],
    Bytes0 = [B7, B6, B5, B4, B3, B2, B1, B0],
    int32(5, [B7, B6, B5, B4]),
    Salt = [B3, B2, B1, B0].

% AuthenticationCleartextPassword
auth_message(password, Bytes) :-
    Bytes = [82,_,_,_,_|Bytes0],
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
    Bytes = [82,0,0,0,8,0,0,0,0].

% Query
query_message(Query, Bytes) :-
    pstring(Query, Bytes0),
    length(Bytes0, L),
    RealLength is L + 4,
    int32(RealLength, Bytes1),
    append([81|Bytes1], Bytes0, Bytes).

% ErrorResponse
error_message(Error, Bytes) :-
    Bytes = [69,_,_,_,_,B0|Bytes0],
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
    Bytes = [67|_].

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
    Length = 4294967295,
    Column = null,
    Fields0 is Fields - 1,
    get_fields(Columns, Fields0, Bytes0).

get_fields([Column|Columns], Fields, Bytes) :-
    Bytes = [B3, B2, B1, B0|Bytes0],
    int32(Length, [B3, B2, B1, B0]),
    Length \= 4294967295,
    take(Length, Bytes0, ColumnBytes, NewBytes),
    chars_utf8bytes(Column, ColumnBytes),
    Fields0 is Fields - 1,
    get_fields(Columns, Fields0, NewBytes).

take(N, Bytes, Bytes0, Bytes1) :-
    append(Bytes0, Bytes1, Bytes),
    length(Bytes0, N).

% ReadyForQuery
ready_for_query_message(Bytes) :-
    Bytes = [90|_].