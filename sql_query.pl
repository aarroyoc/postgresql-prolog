:- module(sql_query, [sql_query/3]).

:- use_module(library(dcgs)).
:- use_module(library(format)).
:- use_module(library(lists)).
:- use_module(library(si)).

sql_query(DSLQuery, TextQuery, Vars) :-
    phrase(sql_query_select(DSLQuery, Vars), TextQuery).

sql_query_select([Select|Rest], Vars) -->
    { Select =.. [select|Args] },
    "SELECT ",
    quoted_comma_separated_list(Args),
    " ",
    sql_query_from(Rest, Vars).

quoted_comma_separated_list([Arg]) -->
    format_("~w", [Arg]).
quoted_comma_separated_list([Arg|Args]) -->
    format_("~w,", [Arg]),
    quoted_comma_separated_list(Args).

sql_query_from([from(Table)], []) -->
    "FROM ",
    format_("~w", [Table]).

sql_query_from([from(Table)|Rest], Vars) -->
    "FROM ",
    format_("~w", [Table]),
    " ",
    sql_query_where(Rest, Vars).

sql_query_where([where(Cond)], Vars) -->
    "WHERE ",
    sql_cond(Cond, [], Vars).

sql_cond((A,B), Vars0, Vars) -->
    "(",
    sql_cond(A, Vars0, Vars1),
    ") AND (",
    sql_cond(B, Vars1, Vars),
    ")".

sql_cond((A;B), Vars0, Vars) -->
    "(",
    sql_cond(A, Vars0, Vars1),
    ") OR (",
    sql_cond(B, Vars1, Vars),
    ")".

sql_cond(A = B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w = ~w", [VarA, VarB]).

%sql_cond(A \= B) -->
%    format_("~w <> ~w", [A, B]).

sql_var(Var, OutVar, Vars0, Vars) :-
    chars_si(Var),
    length(Vars0, N0),
    N is N0 + 1,
    number_chars(N, Cs),
    atom_chars(OutVar, ['$'|Cs]),
    Vars = [N-Var|Vars0].

sql_var(Var, OutVar, Vars0, Vars) :-
    ( atom_si(Var) ; integer_si(Var)),
    Var = OutVar,
    Vars0 = Vars.
