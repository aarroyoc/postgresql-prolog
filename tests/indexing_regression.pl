:- use_module(library(format)).
:- use_module(library(lists)).

% Regression test for the first-argument indexing bug in Scryer 0.9.4
% that motivated the multiplication-based decoding in types.pl
% (int32/2 and int16/2 -- see the comment at types.pl:11-13).
%
% On Scryer 0.9.4, integers produced by `<<` carry a tag that the
% indexed dispatch of multiclause predicates such as auth_method_/3
% fails to match, so the `shift_*` probes below report `solutions=[]`
% even though the numeric value is correct. The `mul_*` probes succeed
% because multiplication keeps the result in the fixnum representation
% the clause-head literals were compiled against.
%
% On Scryer 0.10.0 the regression is fixed: both formulations dispatch
% correctly and every probe reports its expected solution. When 0.10.0
% becomes the supported minimum, the multiplication workaround in
% types.pl can be reverted to the natural `<<` form.
%
% Run with:
%   scryer-prolog tests/indexing_regression.pl
%
% Reproduces the int32/2 decoding both ways: with `<<` (indexing-breaking
% representation on 0.9.4) and with `*` (workaround used in types.pl).
int32_shift(N, [B3,B2,B1,B0]) :-
    N is (B3 << 24) + (B2 << 16) + (B1 << 8) + B0.

int32_mul(N, [B3,B2,B1,B0]) :-
    N is B3*16777216 + B2*65536 + B1*256 + B0.

% Same clause shape as auth_method_/3 in messages.pl.
am_(0,  _,    ok).
am_(3,  _,    password).
am_(5,  Salt, md5(Salt)).
am_(10, Body, sasl(Body)).
am_(11, Body, sasl_continue(Body)).
am_(12, Body, sasl_final(Body)).

:- dynamic(probe_failed/1).

probe(Label, Decoder, Bytes, Expected) :-
    call(Decoder, N, Bytes),
    findall(M, am_(N, [], M), Ms),
    (   Ms = [Sol], \+ \+ Sol = Expected
    ->  Status = pass
    ;   Status = fail,
        assertz(probe_failed(Label))
    ),
    format("~w: bytes=~w -> N=~w, solutions=~w, expected=~w [~w]~n",
           [Label, Bytes, N, Ms, [Expected], Status]).

run :-
    probe(shift_00, int32_shift, [0,0,0,0],  ok),
    probe(mul_00,   int32_mul,   [0,0,0,0],  ok),
    probe(shift_03, int32_shift, [0,0,0,3],  password),
    probe(mul_03,   int32_mul,   [0,0,0,3],  password),
    probe(shift_05, int32_shift, [0,0,0,5],  md5(_)),
    probe(mul_05,   int32_mul,   [0,0,0,5],  md5(_)),
    probe(shift_10, int32_shift, [0,0,0,10], sasl(_)),
    probe(mul_10,   int32_mul,   [0,0,0,10], sasl(_)),
    findall(L, probe_failed(L), Failures),
    (   Failures == []
    ->  format("~nALL PROBES PASSED~n", [])
    ;   format("~nFAILED PROBES: ~w~n", [Failures]),
        fail
    ).

:- initialization((run -> halt(0) ; halt(1))).
