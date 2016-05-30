%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%% GB_HASH register that stores and manages hash rings.
%%% @end
%%% Created :  18 Mar 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash_register).

-behaviour(gen_server).

%% API
-export([start_link/0,
	 insert/2,
	 delete/1,
	 lookup/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-include("gb_hash.hrl").
-include("gb_log.hrl").

-define(Mod, gb_hash_genreg).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Insert a new register entry for Key -> Value mapping to register.
%% Register is a module that provides fast access to Value.
%% To prevent a new atom creation at each new mapping, we modify the
%% existing code.
%% @end
%%--------------------------------------------------------------------
-spec insert(Key :: string(), Value :: term()) ->
    ok | {error, Reason :: term()}.
insert(Key, Value) when is_list(Key)->
    insert(Key, Value, cerl:is_literal_term(Value));
insert(Key, _) ->
    {error, {key_not_list, Key}}.

-spec insert(Key :: string(), Value :: term(), Bool :: true | false) ->
    ok | {error, Reason :: term()}.
insert(Key, Value, true) ->
    gen_server:call(?MODULE, {register, Key, Value});
insert(_, Value, false) ->
    {error, {data_not_literal, Value}}.

%%--------------------------------------------------------------------
%% @doc
%% Delete a register entry for mapping specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Key :: string()) ->
    ok | {error, Reason ::term()}.
delete(Key) ->
    gen_server:call(?MODULE, {unregister, Key}).


%%--------------------------------------------------------------------
%% @doc
%% Lookup for a register entry specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec lookup(Key :: string()) ->
    Value :: term() | undefined.
lookup(Key) ->
    ?Mod:lookup(Key).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ok = mnesia:wait_for_tables([gb_hash_register], 20000),
    Entries = registry_to_list(),
    ok = regen_register(Entries),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({register, Name, Func}, _From, State) ->
    Transaction =
        fun() ->
	    ok = mnesia:write(#gb_hash_register{name = Name, func = Func}),
	    Entries = registry_to_list(),
	    ok = regen_register(Entries)
	end,
    Reply =
	case gb_hash_db:transaction(Transaction) of
	    {atomic, ok} ->
		ok;
	    {error, Reason} ->
		{error, Reason}
	end,
    {reply, Reply, State};
handle_call({unregister, Name}, _From, State) ->
    Transaction =
        fun() ->
	    ok = mnesia:delete(gb_hash_register, Name, write),
	    Entries = registry_to_list(),
	    ok = regen_register(Entries)
	end,
    Reply = 
	case gb_hash_db:transaction(Transaction) of
	    {atomic, ok} ->
		ok;
	    {error, Reason} ->
		{error, Reason}
	end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
   ?debug("terminating.", []),
   ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec registry_to_list() ->
    [{Name :: string(), Func :: term()}] | {error, Reason :: term()}.
registry_to_list() ->
    case mnesia:is_transaction() of
	true ->
	    mnesia:foldl(fun reg_traverse/2, [], gb_hash_register);
	false ->
    	    Transaction =
	        fun() ->
		    mnesia:foldl(fun reg_traverse/2, [], gb_hash_register)
		end,
	    case gb_hash_db:transaction(Transaction) of
		{atomic, NewAcc} ->
		    lists:reverse(NewAcc);
		{error, Reason} ->
		    {error, Reason}
	    end
    end.

-spec reg_traverse(Rec :: #gb_hash_register{},
		   Acc :: [{string(), term()}]) ->
    [{string(), term()}].
reg_traverse(#gb_hash_register{name = Name, func = Func}, Acc) ->
    [{Name, Func} | Acc].

%%--------------------------------------------------------------------
%% @doc
%% Re-generate register with given Entries.
%% @end
%%--------------------------------------------------------------------
-spec regen_register(Entries :: [{string(), term()}]) ->
    ok.
regen_register(Entries) ->
    CEForms = make_mod(Entries),
    {ok, _, Beam} = compile:forms(CEForms, [from_core, binary]),
    {module, _ } = code:load_binary(?Mod, [], Beam),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Make module 'gb_hash_genreg' with lookup function that matches
%% terms in Entries.
%% @end
%%--------------------------------------------------------------------
-spec make_mod(Entries :: [{Key :: string(), Value :: term()}]) ->
    term().
make_mod(Entries) ->
    ModuleName = cerl:c_atom(?Mod),
    cerl:c_module(ModuleName,
		  [cerl:c_fname(lookup, 1),
		   cerl:c_fname(module_info, 0),
		   cerl:c_fname(module_info, 1)],
		  [make_lookup_fun(Entries) | mod_info(ModuleName)]).

%%--------------------------------------------------------------------
%% @doc
%% Make lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_fun(Entries) ->
    Arg1 = cerl:c_var('FuncArg1'),
    Else = cerl:c_var('Else'),
    True = cerl:c_atom(true),
    Undefined = cerl:c_atom(undefined),

    Clauses = make_lookup_clauses(Arg1, Entries),
    
    LastClause = cerl:c_clause([Else], True, Undefined),
    Case = cerl:c_case(Arg1, Clauses ++ [LastClause]),
    {cerl:c_fname(lookup,1), cerl:c_fun([Arg1], Case)}.

%%--------------------------------------------------------------------
%% @doc
%% Make case clauses for lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_clauses(Arg1, Entries) ->
    make_lookup_clauses(Arg1, Entries, []).

%%--------------------------------------------------------------------
%% @doc
%% Make case clauses for lookup/1 function.
%% @end
%%--------------------------------------------------------------------
make_lookup_clauses(_Arg1, [], Acc) ->
    lists:reverse(Acc);
make_lookup_clauses(Arg1, [{Key, Value} | Rest], Acc) ->
    Pattern = [cerl:c_string(Key)],
    Guard = cerl:c_atom(true),
    Body = cerl:abstract(Value), 
    Clause = cerl:c_clause(Pattern, Guard, Body),
    make_lookup_clauses(Arg1, Rest, [Clause|Acc]).

%%--------------------------------------------------------------------
%% @doc
%% Make module_info/1 function.
%% @end
%%--------------------------------------------------------------------
mod_info(Name) ->
    M = cerl:c_atom(erlang),
    F = cerl:c_atom(get_module_info),
    Info0 = {cerl:c_fname(module_info, 0),
	     cerl:c_fun([], cerl:c_call(M, F, [Name]))},
    Key = cerl:c_var('Key'),
    Info1 = {cerl:c_fname(module_info, 1),
	     cerl:c_fun([Key], cerl:c_call(M, F, [Name, Key]))},
    [Info0, Info1].

%%--------------------------------------------------------------------
%% %@doc
%% Make a function call cerl with given Mod, Fun, and Args.
%% %@end
%%--------------------------------------------------------------------
%%make_call(Mod0, Fun0, Args) ->
%%    Mod = cerl:c_atom(Mod0),
%%    Fun = cerl:c_atom(Fun0),
%%    cerl:c_call(Mod, Fun, Args).

%%--------------------------------------------------------------------
%% %@doc
%% Make lookup/1 function using maps.
%% %@end
%%--------------------------------------------------------------------
%%make_lookup_fun(Entries) ->
%%    Arg1 = cerl:c_var('FuncArg1'),
%%    Undefined = cerl:c_atom(undefined),
%%    MapPairs = make_map_pairs(Entries, []),
%%    Map = cerl:c_map(MapPairs),
%%    MapLookup = make_call(maps, get, [Arg1, Map, Undefined]),
%%    {cerl:c_fname(lookup,1), cerl:c_fun([Arg1], MapLookup)}.
%%
%%make_map_pairs([{Key, Value} | Rest], Acc) ->
%%    MapPair = cerl:c_map_pair(cerl:c_string(Key), cerl:abstract(Value)),
%%    make_map_pairs(Rest, [MapPair | Acc]);
%%make_map_pairs([], Acc) ->
%%    Acc.
