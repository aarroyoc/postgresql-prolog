:- module(scram, [do_scram_sha_256_after_offer/3]).

/**
SCRAM-SHA-256 SASL authentication for the PostgreSQL wire protocol.

Implements the client side of the SASL exchange specified by
[RFC 5802](https://www.rfc-editor.org/rfc/rfc5802) and
[RFC 7677](https://www.rfc-editor.org/rfc/rfc7677), as carried by
PostgreSQL's `AuthenticationSASL` / `AuthenticationSASLContinue` /
`AuthenticationSASLFinal` messages.

The handshake exchanges four SASL messages:

- *client-first* (`SASLInitialResponse`) carrying the client nonce
- *server-first* (`R(11) AuthenticationSASLContinue`) with the combined
  nonce, salt and iteration count
- *client-final* (`SASLResponse`) carrying the client proof
- *server-final* (`R(12) AuthenticationSASLFinal`) with the server signature

The module uses `gs2-cbind-flag = "n"` (no channel binding) and omits the
authzid, so the GS2 header is `n,,` and the base64 channel binding is
`biws`. Cryptographic primitives come from `library(crypto)`; PBKDF2 is
implemented locally on top of `hmac_sha256/3`.
*/

:- use_module(library(charsio)).
:- use_module(library(crypto)).
:- use_module(library(lists)).
:- use_module(library(format)).
:- use_module(library(dcgs)).
:- use_module(library(reif)).

:- use_module(messages, [
    auth_method/2,
    sasl_initial_response_message/3,
    sasl_response_message/2
]).

% Reads bytes from Stream the same way postgresql.pl get_bytes/2 does.
:- use_module(types, [int32/2]).

% ---------- SCRAM message grammar (RFC 5802 §7) ------------------
% We use gs2-cbind-flag = "n" (no channel binding) and omit authzid,
% so gs2-header = "n,," and channel-binding base64 = "biws".
% Each rule below mirrors one ABNF production.

client_first_message(User, Nonce) -->
    gs2_header,
    client_first_message_bare(User, Nonce).

gs2_header --> "n,,".

client_first_message_bare(User, Nonce) -->
    username(User), ",", nonce(Nonce).

username(SaslName) --> "n=", seq(SaslName).
nonce(Printable)   --> "r=", seq(Printable).

channel_binding --> "c=biws".

client_final_message_without_proof(Nonce) -->
    channel_binding, ",", nonce(Nonce).

proof(Base64) --> "p=", seq(Base64).

client_final_message(Nonce, Base64Proof) -->
    client_final_message_without_proof(Nonce), ",", proof(Base64Proof).

% AuthMessage = client-first-message-bare "," server-first-message ","
%               client-final-message-without-proof
auth_message(User, ClientNonce, ServerFirst, CombinedNonce) -->
    client_first_message_bare(User, ClientNonce), ",",
    seq(ServerFirst), ",",
    client_final_message_without_proof(CombinedNonce).

%% do_scram_sha_256_after_offer(+Stream, +User, +Password)
%
% Performs SCRAM-SHA-256 SASL authentication on a PostgreSQL wire-protocol
% `Stream` that has just received an `R(10) AuthenticationSASL` message
% advertising `SCRAM-SHA-256`.
%
% Sends `SASLInitialResponse` with the client-first message, parses the
% server-first message returned in `R(11)`, derives the salted password via
% PBKDF2-HMAC-SHA-256, sends the client proof in `SASLResponse`, and finally
% verifies the server signature returned in `R(12) AuthenticationSASLFinal`.
%
% On success the stream is left positioned to read the next
% `R(0) AuthenticationOk`, after which the caller should proceed to
% `ReadyForQuery`.
%
% Throws:
%
% - `scram_invalid_server_nonce` when the combined nonce does not extend the
%   client nonce
% - `scram_server_signature_mismatch` when the server signature does not match
%   the value expected from `ServerKey`
% - `scram_server_error(Err)` when the server reports an `e=<error>` in the
%   final message
%
% References:
%
% - [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802) (SCRAM)
% - [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677) (SCRAM-SHA-256)
% - PostgreSQL protocol: SASL Authentication
do_scram_sha_256_after_offer(Stream, User, Password) :-
    % --- client-first-message ---
    client_nonce_chars(ClientNonce),
    phrase(client_first_message(User, ClientNonce), ClientFirst),

    sasl_initial_response_message("SCRAM-SHA-256", ClientFirst, InitBytes),
    put_bytes(Stream, InitBytes),

    % --- server-first-message in R(11) ---
    get_message_bytes(Stream, ContinueMsg),
    auth_method(ContinueMsg, sasl_continue(ServerFirstBytes)),
    chars_utf8bytes(ServerFirst, ServerFirstBytes),
    parse_server_first(ServerFirst, ServerNonce, SaltBytes, Iterations),

    % Sanity: server must echo our client nonce as a prefix.
    (   append(ClientNonce, _, ServerNonce)
    ->  true
    ;   throw(scram_invalid_server_nonce) ),

    % --- compute keys ---
    chars_utf8bytes(Password, PasswordBytes),
    pbkdf2_hmac_sha256(PasswordBytes, SaltBytes, Iterations, 32, SaltedPassword),

    hmac_sha256(SaltedPassword, "Client Key", ClientKey),
    sha256_bytes(ClientKey, StoredKey),

    % AuthMessage = client-first-message-bare "," server-first-message ","
    %               client-final-message-without-proof  (RFC 5802 §3)
    phrase(auth_message(User, ClientNonce, ServerFirst, ServerNonce), AuthMessageChars),

    hmac_sha256(StoredKey, AuthMessageChars, ClientSignature),
    bytes_xor(ClientKey, ClientSignature, ClientProof),

    bytes_to_chars(ClientProof, ClientProofChars),
    chars_base64(ClientProofChars, ClientProofB64, []),

    phrase(client_final_message(ServerNonce, ClientProofB64), ClientFinal),

    sasl_response_message(ClientFinal, FinalBytes),
    put_bytes(Stream, FinalBytes),

    % --- server-final-message in R(12) ---
    get_message_bytes(Stream, FinalMsg),
    auth_method(FinalMsg, sasl_final(ServerFinalBytes)),
    chars_utf8bytes(ServerFinal, ServerFinalBytes),
    parse_server_final(ServerFinal, ServerSigB64),
    chars_base64(ServerSigChars, ServerSigB64, []),
    chars_to_bytes(ServerSigChars, ServerSigBytes),

    % Verify server signature
    hmac_sha256(SaltedPassword, "Server Key", ServerKey),
    hmac_sha256(ServerKey, AuthMessageChars, ExpectedServerSig),

    if_(dif(ServerSigBytes, ExpectedServerSig),
        throw(scram_server_signature_mismatch),
        true
    ).


% ---------------------------------------------------------------- helpers

%% get_message_bytes(+Stream, -Bytes)
%
% Reads a single PostgreSQL protocol message from `Stream` as the byte list
% `[TypeByte, L3, L2, L1, L0, Body...]`, where the `int32(L3..L0)` length
% includes itself but not the type byte.
get_message_bytes(Stream, Bytes) :-
    get_byte(Stream, BType),
    get_byte(Stream, B3),
    get_byte(Stream, B2),
    get_byte(Stream, B1),
    get_byte(Stream, B0),
    int32(Length, [B3,B2,B1,B0]),
    Remaining is Length - 4,
    get_n_bytes(Stream, Remaining, Body),
    append([BType,B3,B2,B1,B0], Body, Bytes).

get_n_bytes(_, 0, []) :- !.
get_n_bytes(Stream, N, [B|Bs]) :-
    N > 0,
    get_byte(Stream, B),
    N1 is N - 1,
    get_n_bytes(Stream, N1, Bs).

put_bytes(_, []).
put_bytes(Stream, [B|Bs]) :-
    put_byte(Stream, B),
    put_bytes(Stream, Bs).

%% client_nonce_chars(-Nonce)
%
% A printable-ASCII nonce: 18 random bytes base64-encoded with the URL
% charset and no padding (24 chars, no `/`).
client_nonce_chars(Nonce) :-
    crypto_n_random_bytes(18, RandBytes),
    bytes_to_chars(RandBytes, RandChars),
    chars_base64(RandChars, NonceWithMaybeSlash, [padding(false), charset(url)]),
    Nonce = NonceWithMaybeSlash.

bytes_to_chars([], []).
bytes_to_chars([B|Bs], [C|Cs]) :- char_code(C, B), bytes_to_chars(Bs, Cs).

chars_to_bytes([], []).
chars_to_bytes([C|Cs], [B|Bs]) :- char_code(C, B), chars_to_bytes(Cs, Bs).

%% bytes_xor(+As, +Bs, -Cs)
%
% Element-wise XOR of two byte lists of equal length: `C[i] = A[i] xor B[i]`.
bytes_xor([], [], []).
bytes_xor([A|As], [B|Bs], [C|Cs]) :-
    C is xor(A, B),
    bytes_xor(As, Bs, Cs).

%% sha256_bytes(+InputBytes, -OutputBytes)
%
% SHA-256 over a byte list, returning the 32-byte digest as a byte list.
sha256_bytes(InputBytes, OutputBytes) :-
    bytes_to_chars(InputBytes, InputChars),
    crypto_data_hash(InputChars, Hex, [algorithm(sha256), encoding(octet)]),
    hex_bytes(Hex, OutputBytes).

%% hmac_sha256(+KeyBytes, +Data, -MacBytes)
%
% HMAC-SHA-256 with key `KeyBytes` (a byte list) over `Data`. `Data` may be
% either a list of chars or a list of bytes (0..255); byte input is converted
% to octet-chars before being fed to `crypto_data_hash/3` with
% `encoding(octet)`.
hmac_sha256(KeyBytes, DataChars, MacBytes) :-
    (   data_is_bytes(DataChars)
    ->  bytes_to_chars(DataChars, OctetChars)
    ;   OctetChars = DataChars ),
    crypto_data_hash(OctetChars, Hex, [algorithm(sha256), hmac(KeyBytes), encoding(octet)]),
    hex_bytes(Hex, MacBytes).

%% data_is_bytes(+List)
%
% Succeeds when `List` looks like a byte list: a non-empty list whose first
% element is an integer in `0..255`. Used by `hmac_sha256/3` to decide whether
% to convert input to octet-chars.
data_is_bytes([H|_]) :- integer(H), H >= 0, H =< 255.

%% pbkdf2_hmac_sha256(+PasswordBytes, +SaltBytes, +Iterations, +DkLen, -DK)
%
% PBKDF2 with HMAC-SHA-256 as the underlying PRF, per
% [RFC 8018](https://www.rfc-editor.org/rfc/rfc8018). Only `DkLen = 32` (a
% single output block) is supported, which is what SCRAM-SHA-256 requires.
pbkdf2_hmac_sha256(Password, Salt, Iterations, 32, DK) :-
    int32_be_bytes(1, IndexBytes),
    append(Salt, IndexBytes, FirstBlockInput),
    hmac_sha256(Password, FirstBlockInput, U1),
    pbkdf2_iterate(Password, U1, U1, Iterations, 1, DK).

pbkdf2_iterate(_, _, Acc, MaxIter, MaxIter, Acc) :- !.
pbkdf2_iterate(Password, Prev, Acc, MaxIter, I, DK) :-
    I < MaxIter,
    hmac_sha256(Password, Prev, Next),
    bytes_xor(Acc, Next, Acc1),
    I1 is I + 1,
    pbkdf2_iterate(Password, Next, Acc1, MaxIter, I1, DK).

%% int32_be_bytes(+N, -Bytes)
%
% Encodes a 32-bit non-negative integer `N` as a big-endian 4-byte list.
int32_be_bytes(N, [B3,B2,B1,B0]) :-
    B0 is N /\ 255,
    B1 is (N >> 8) /\ 255,
    B2 is (N >> 16) /\ 255,
    B3 is (N >> 24) /\ 255.

%% parse_server_first(+Chars, -CombinedNonce, -SaltBytes, -Iterations)
%
% Parses a server-first message of the form
% `r=<nonce>,s=<base64-salt>,i=<iter>`, returning the combined nonce, the
% decoded salt as bytes, and the iteration count as an integer. The optional
% `m=` mandatory-extensions attribute is ignored if present.
parse_server_first(Chars, Nonce, SaltBytes, Iterations) :-
    split_on(Chars, ',', Parts),
    member(NoncePart, Parts), append("r=", Nonce, NoncePart), !,
    member(SaltPart, Parts), append("s=", SaltB64, SaltPart), !,
    member(IterPart, Parts), append("i=", IterChars, IterPart), !,
    chars_base64(SaltChars, SaltB64, []),
    chars_to_bytes(SaltChars, SaltBytes),
    number_chars(Iterations, IterChars).

%% parse_server_final(+Chars, -ServerSigB64)
%
% On success, extracts the `v=<base64-signature>` server signature from a
% server-final message. Throws `scram_server_error(Err)` when the server
% reported an `e=<error>` instead.
parse_server_final(Chars, ServerSigB64) :-
    split_on(Chars, ',', Parts),
    (   member(EPart, Parts), append("e=", Err, EPart)
    ->  throw(scram_server_error(Err))
    ;   true ),
    member(VPart, Parts), append("v=", ServerSigB64, VPart), !.

%% split_on(+Chars, +SepChar, -Parts)
%
% Splits the char list `Chars` on every occurrence of `SepChar`, producing
% `Parts` as the list of separator-free segments (a trailing empty segment is
% omitted because the base case returns the remainder as a single part).
split_on(Chars, Sep, [Part|Rest]) :-
    append(Part, [Sep|Tail], Chars),
    \+ member(Sep, Part),
    !,
    split_on(Tail, Sep, Rest).
split_on(Chars, _, [Chars]).
