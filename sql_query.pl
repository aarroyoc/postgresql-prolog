:- module(sql_query, [sql_query/3]).

:- use_module(library(dcgs)).
:- use_module(library(format)).
:- use_module(library(lists)).
:- use_module(library(si)).

% Documentation
% SQL-92 BNF: https://ronsavage.github.io/SQL/sql-92.bnf.html
% This translation doesn't try to be complete. Just enough for basic use cases.

sql_query(DSLQuery, TextQuery, Vars) :-
    phrase(sql_query_select(DSLQuery, Vars), TextQuery).

sql_query(DSLQuery, TextQuery, Vars) :-
    phrase(sql_query_insert(DSLQuery, Vars), TextQuery).

sql_query(DSLQuery, TextQuery, Vars) :-
    phrase(sql_query_update(DSLQuery, Vars), TextQuery).

sql_query(DSLQuery, TextQuery, Vars) :-
    phrase(sql_query_delete(DSLQuery, Vars), TextQuery).

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

sql_query_from([from(Table)], []  ) -->
    "FROM ",
    format_("~w", [Table]).

sql_query_from([from(Table)|Rest], Vars) -->
    "FROM ",
    format_("~w", [Table]),
    " ",
    sql_query_join(Rest, Vars),
    { var(Vars) -> Vars = []; true }.

sql_query_join([join(Table)|Rest], Vars) -->
    sql_query_join([inner_join(Table)|Rest], Vars).

sql_query_join([inner_join(Table)|Rest], Vars) -->
    "INNER JOIN ",
    format_("~w", [Table]),
    " ",
    sql_query_join_second(Rest, Vars).

sql_query_join([left_join(Table)|Rest], Vars) -->
    sql_query_join([left_outer_join(Table)|Rest], Vars).

sql_query_join([left_outer_join(Table)|Rest], Vars) -->
    "LEFT OUTER JOIN ",
    format_("~w", [Table]),
    " ",
    sql_query_join_second(Rest, Vars).

sql_query_join([right_join(Table)|Rest], Vars) -->
    sql_query_join([right_outer_join(Table)|Rest], Vars).

sql_query_join([right_outer_join(Table)|Rest], Vars) -->
    "RIGHT OUTER JOIN ",
    format_("~w", [Table]),
    " ",
    sql_query_join_second(Rest, Vars).

sql_query_join([full_join(Table)|Rest], Vars) -->
    sql_query_join([full_outer_join(Table)|Rest], Vars).

sql_query_join([full_outer_join(Table)|Rest], Vars) -->
    "FULL OUTER JOIN ",
    format_("~w", [Table]),
    " ",
    sql_query_join_second(Rest, Vars).

sql_query_join([natural_join(Table)], []) -->
    "NATURAL JOIN ",
    format_("~w", [Table]).

sql_query_join([natural_join(Table)|Rest], Vars) -->
    "NATURAL JOIN ",
    format_("~w", [Table]),
    " ",
    sql_query_join(Rest, Vars).

sql_query_join(Rest, Vars) -->
    sql_query_where(Rest, Vars).

sql_query_join_second([on(Cond)], Vars) -->
    "ON ",
    sql_cond(Cond, [], Vars).

sql_query_join_second([on(Cond)|Rest], Vars) -->
    "ON ",
    sql_cond(Cond, [], Vars0),
    " ",
    sql_query_join(Rest, Vars1),
    { append(Vars0, Vars1, Vars) }.

sql_query_join_second([using(Col)], []) -->
    "USING (",
    format_("~w", [Col]),
    ")".

sql_query_join_second([using(Col)|Rest], Vars) -->
    "USING (",
    format_("~w", [Col]),
    ") ",
    sql_query_join(Rest, Vars).

sql_query_where([where(Cond)], Vars) -->
    "WHERE ",
    sql_cond(Cond, [], Vars).

sql_query_where([where(Cond)|Rest], Vars) -->
    "WHERE ",
    sql_cond(Cond, [], Vars),
    " ",
    sql_query_group_by(Rest).

sql_query_where(Rest, []) -->
    sql_query_group_by(Rest).

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

sql_cond((\+ A), Vars0, Vars) -->
    "NOT (",
    sql_cond(A, Vars0, Vars),
    ")".

sql_cond(is_null(A), Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars) },
    format_("~w IS NULL", [VarA]).

sql_cond(A = B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w = ~w", [VarA, VarB]).

sql_cond(A \= B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w <> ~w", [VarA, VarB]).

sql_cond(A > B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w > ~w", [VarA, VarB]).

sql_cond(A < B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w < ~w", [VarA, VarB]).

sql_cond(A >= B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w >= ~w", [VarA, VarB]).

sql_cond(A =< B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w <= ~w", [VarA, VarB]).


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

sql_query_group_by([GroupBy]) -->
    { GroupBy =.. [group_by|Args] },
    "GROUP BY ",
    quoted_comma_separated_list(Args).

sql_query_group_by([GroupBy|Rest]) -->
    { GroupBy =.. [group_by|Args] },
    "GROUP BY ",
    quoted_comma_separated_list(Args),
    sql_query_order_by(Rest).

sql_query_group_by(Rest) -->
    sql_query_order_by(Rest).

sql_query_order_by([OrderBy]) -->
    { OrderBy =.. [order_by|Args0] },
    "ORDER BY ",
    { order_args(Args0, Args) },
    quoted_comma_separated_list(Args).

sql_query_order_by([OrderBy|Rest]) -->
    { OrderBy =.. [order_by|Args0] },
    "ORDER BY ",
    { order_args(Args0, Args) },
    quoted_comma_separated_list(Args),
    " ",
    sql_query_offset(Rest).

sql_query_order_by(Rest) -->
    sql_query_offset(Rest).

sql_query_offset([offset(N)]) -->
    "OFFSET ",
    format_("~w", [N]),
    " ROWS".

sql_query_offset([offset(N)|Rest]) -->
    "OFFSET ",
    format_("~w", [N]),
    " ROWS ",
    sql_query_fetch_first(Rest).

sql_query_offset(Rest) -->
    sql_query_fetch_first(Rest).

sql_query_fetch_first([fetch_first(N, only)]) -->
    "FETCH FIRST ",
    format_("~w", [N]),
    " ROWS ONLY".

sql_query_fetch_first([fetch_first(N, with_ties)]) -->
    "FETCH FIRST ",
    format_("~w", [N]),
    " ROWS WITH TIES".

order_args([], []).
order_args([asc(Arg0)|Rest0], [Arg|Rest]) :-
    atom_concat(Arg0, ' ASC', Arg),
    order_args(Rest0, Rest).
order_args([desc(Arg0)|Rest0], [Arg|Rest]) :-
    atom_concat(Arg0, ' DESC', Arg),
    order_args(Rest0, Rest).

% INSERT

sql_query_insert([insert_into(Table, Cols)|Values], Vars) -->
    "INSERT INTO ",
    format_("~w", [Table]),
    " (",
    quoted_comma_separated_list(Cols),
    ") ",
    sql_query_insert_values(Values, Vars).

sql_query_insert_values([Values0|Next], Vars) -->
    { Values0 =.. [values|Values] },
    "VALUES (",
    { values_args(Values, Args, [], Vars0) },
    quoted_comma_separated_list(Args),
    ") ",
    sql_query_insert_on_conflict(Next, Vars1),
    { append(Vars0, Vars1, Vars) }.

sql_query_insert_values([Values0], Vars) -->
    { Values0 =.. [values|Values] },
    "VALUES (",
    { values_args(Values, Args, [], Vars) },
    quoted_comma_separated_list(Args),
    ")".

sql_query_insert_on_conflict([Values0|Next], Vars) -->
    { Values0 =.. [on_conflict_do_update|Cols] },
    "ON CONFLICT (",
    quoted_comma_separated_list(Cols),
    ") DO UPDATE ",
    sql_query_insert_do_update_set(Next, Vars).

sql_query_insert_on_conflict(Rest, []) -->
    sql_query_insert_returning(Rest).

sql_query_insert_do_update_set([set(Cond)], Vars) -->
    "SET ",
    sql_set(Cond, [], Vars).

sql_query_insert_do_update_set([set(Cond) | Rest], Vars) -->
    "SET ",
    sql_set(Cond, [], Vars),
    " ",
    sql_query_insert_returning(Rest).

sql_query_insert_returning([Returning0]) -->
    { Returning0 =.. [returning|Columns] },
    "RETURNING ",
    quoted_comma_separated_list(Columns).

values_args([], [], X, X).
values_args([Value|Values], [Arg|Args], Vars0, Vars) :-
    sql_var(Value, Arg, Vars0, Vars1),
    values_args(Values, Args, Vars1, Vars).

% UPDATE

sql_query_update([update(Table)|Rest], Vars) -->
    "UPDATE ",
    format_("~w", [Table]),
    " ",
    sql_query_update_set(Rest, Vars).

sql_query_update_set([set(Cond)], Vars) -->
    "SET ",
    sql_set(Cond, [], Vars).

sql_query_update_set([set(Cond)|Rest], Vars) -->
    "SET ",
    sql_set(Cond, [], Vars1),
    " ",
    sql_query_update_where(Rest, Vars1, Vars).

sql_query_update_where([where(Cond)], Vars0, Vars) -->
    "WHERE ",
    sql_cond(Cond, Vars0, Vars).

sql_set((A,B), Vars0, Vars) -->
    sql_set(A, Vars0, Vars1),
    ",",
    sql_set(B, Vars1, Vars).

sql_set(A = B, Vars0, Vars) -->
    { sql_var(A, VarA, Vars0, Vars1) },
    { sql_var(B, VarB, Vars1, Vars) },
    format_("~w = ~w", [VarA, VarB]).

% DELETE

sql_query_delete([delete(Table)|Rest], Vars) -->
    "DELETE FROM ",
    format_("~w", [Table]),
    " ",
    sql_query_delete_where(Rest, Vars).

sql_query_delete_where([where(Cond)], Vars) -->
    "WHERE ",
    sql_cond(Cond, [], Vars).
