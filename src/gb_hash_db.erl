%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%% Database module to manage mnesia tables those are used by gb_hash application.
%%% @end
%%% Created :  18 Mar 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash_db).

%% API
-export([create_tables/1]).

-export([transaction/1]).

-include("gb_hash.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Create tables on given Nodes.
%% @end
%%--------------------------------------------------------------------
-spec create_tables(Nodes :: [node()]) -> ok | {error, Reason :: any()}.
create_tables(Nodes) ->
    [create_table(Nodes, T) || T <- [gb_hash_register]].

%%--------------------------------------------------------------------
%% @doc
%% Run mnesia activity with access context transaction with given fun
%% @end
%%--------------------------------------------------------------------
-spec transaction(Fun :: fun()) -> {aborted, Reason::term()} | {atomic, ResultOfFun::term()}.
transaction(Fun) ->
    case catch mnesia:activity(transaction, Fun) of
	{'EXIT', Reason} ->
	    {error, Reason};
	Result ->
	    {atomic, Result}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec create_table(Nodes::[node()], Name::atom()) -> ok | {error, Reason::term()}.
create_table(Nodes, Name) when Name == gb_hash_register->
    TabDef = [{access_mode, read_write},
	      {attributes, record_info(fields, gb_hash_register)},
	      {disc_copies, Nodes},
	      {load_order, 50},
	      {record_name, Name},
	      {type, set}
	     ],
    mnesia:create_table(Name, TabDef);
create_table(_, _) ->
    {error, "Unknown table definition"}.
