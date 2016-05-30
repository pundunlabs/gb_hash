-module(gb_hash_test).

-revision('$Revision: $ ').
-modified('$Date: $ ').

-include_lib("eunit/include/eunit.hrl").
-include("gb_hash.hrl").

-export([count_occurrences/1]).

%%--------------------------------------------------------------------
%% @doc
%% Test to count the number of keys distributed each shard of a 
%% given.
%% Number of keys are hard coded to 100000.
%% @end
%%--------------------------------------------------------------------
-spec count_occurrences(Table :: string()) -> [{Shard::term(), Count::integer()}].
count_occurrences(Table) ->
    random:seed(os:timestamp()),
    List = [begin
	    {ok, Shard} = gb_hash:find_node(Table, random:uniform(10000000)),
	    Shard
	    end || _ <- lists:seq(1,100000)],
    count_occurrences(List, []).

-spec count_occurrences(Shards::[term()], Acc::[{Shard::term(), Count::integer()}]) ->
    [{Shard::string(), Count::integer()}].
count_occurrences([H|T], Aux) ->
    case lists:keyfind(H, 1, Aux) of 
	false ->
	    count_occurrences(T, [{H, 1} |Aux]);
	{H, C} ->
	    count_occurrences(T, lists:keyreplace(H, 1, Aux, {H,C+1}))
    end;
count_occurrences([], Aux) ->
    Aux.
