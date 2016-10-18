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
-export([start_link/1,
	 insert/3,
	 delete/2,
	 lookup/2,
	 all/1]).

%% Inter Node API
-export([load_store/2,
	 load_store_ok/1,
	 revert/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-include("gb_hash.hrl").
-include("gb_log.hrl").

-record(state, {mod,
		filename}).

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
-spec insert(Reg :: atom(), Key :: string(), Value :: term()) ->
    {ok, Beam :: binary()} | {error, Reason :: term()}.
insert(Reg, Key, Value) when is_list(Key)->
    insert(Reg, Key, Value, is_literal_term(Value));
insert(_, Key, _) ->
    {error, {key_not_list, Key}}.

-spec insert(Reg :: atom(),
	     Key :: string(),
	     Value :: term(),
	     Bool :: true | false) ->
    {ok, Beam :: binary()} | {error, Reason :: term()}.
insert(Reg, Key, Value, true) ->
    gen_server:call(Reg, {register, Key, Value});
insert(_, _, Value, false) ->
    {error, {data_not_literal, Value}}.

%%--------------------------------------------------------------------
%% @doc
%% Delete a register entry for mapping specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec delete(Reg :: atom(), Key :: string()) ->
    {ok, Beam :: binary()} | {error, Reason ::term()}.
delete(Reg, Key) ->
    gen_server:call(Reg, {unregister, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Lookup for a register entry specified by Key.
%% @end
%%--------------------------------------------------------------------
-spec lookup(Mod :: module(), Key :: string()) ->
    Value :: term() | undefined.
lookup(Mod, Key) ->
    Mod:lookup(Key).

%%--------------------------------------------------------------------
%% @doc
%% Lookup for all entries in given module.
%% @end
%%--------------------------------------------------------------------
-spec all(Mod :: module()) ->
    Value :: term() | undefined.
all(Mod) ->
    Mod:entries().

%%--------------------------------------------------------------------
%% @doc
%% Load and Store new register module and return ok.
%% @end
%%--------------------------------------------------------------------
-spec load_store_ok(Beam :: binary()) ->
    ok | {error, Reason ::term()}.
load_store_ok(Beam) when is_binary(Beam) ->
    case load_store(?DistReg, Beam) of
	{ok, _} -> ok;
	Else -> Else
    end.

%%--------------------------------------------------------------------
%% @doc
%% Load and Store new register module.
%% @end
%%--------------------------------------------------------------------
-spec load_store(Reg :: atom(), Beam :: binary()) ->
    {ok, Beam :: binary()} | {error, Reason ::term()}.
load_store(Reg, Beam) when is_binary(Beam) ->
    gen_server:call(Reg, {load_store, Beam}).

%%--------------------------------------------------------------------
%% @doc
%% Revert the hash register to CommitID.
%% @end
%%--------------------------------------------------------------------
-spec revert(CommitID :: term()) ->
    {ok, Beam :: binary()} | {error, Reason ::term()}.
revert(CommitID) ->
    gen_server:call(?MODULE, {revert, CommitID}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    Name = proplists:get_value(name, Args),
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

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
init(Args) ->
    Mod = proplists:get_value(mod, Args),
    File = proplists:get_value(file, Args),
    RootDir = gb_conf_env:proddir(),
    Filename = RootDir ++ File,
    case file:read_file(Filename) of
	{ok, Bin} ->
	    {ok, _} = load_register(Mod, Bin);
	{error,enoent} ->
	    {ok, _} = regen_register(Mod, Filename, [])
    end,
    {ok, #state{mod = Mod,
		filename = Filename}}.

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
    Mod = State#state.mod,
    Filename = State#state.filename,
    Entries = Mod:entries(),
    Reply = regen_register(Mod, Filename, [{Name, Func} | Entries]),
    {reply, Reply, State};
handle_call({unregister, Name}, _From, State) ->
    Mod = State#state.mod,
    Filename = State#state.filename,
    Entries = Mod:entries(),
    Reply = regen_register(Mod, Filename, lists:keydelete(Name, 1, Entries)),
    {reply, Reply, State};
handle_call({load_store, Beam}, _From, State) ->
    store_beam(State#state.filename, Beam),
    Reply = load_register(State#state.mod, Beam),
    {reply, Reply, State};
handle_call({revert, _CommitID}, _From, State) ->
    Reply = ?debug("Revert functionality is not supported yet.",[]),
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

%%--------------------------------------------------------------------
%% @doc
%% Re-generate register with given Entries.
%% @end
%%--------------------------------------------------------------------
-spec regen_register(Mod :: module(),
		     Filename :: string(),
		     Entries :: [{string(), term()}]) ->
    {ok, Beam :: binary()}.
regen_register(Mod, Filename, Entries) ->
    CEForms = make_mod(Mod, Entries),
    {ok, _, Beam} = compile:forms(CEForms, [from_core, binary]),
    store_beam(Filename, Beam),
    load_register(Mod, Beam).

%%--------------------------------------------------------------------
%% @doc
%% Write object code to file to store persistent.
%% @end
%%--------------------------------------------------------------------
-spec store_beam(Filename :: string(),
		 Bin :: binary()) ->
    ok.
store_beam(Filename, Bin) ->
    file:write_file(Filename, Bin).

%%--------------------------------------------------------------------
%% @doc
%% Load object code of register module code on node().
%% @end
%%--------------------------------------------------------------------
-spec load_register(Mod :: module(),
		    Bin :: binary()) ->
    {ok, Bin :: binary()}.
load_register(Mod, Bin) ->
    {module, _ } = code:load_binary(Mod, [], Bin),
    {ok, Bin}.

%%--------------------------------------------------------------------
%% @doc
%% Make module 'gb_hash_genreg' with lookup function that matches
%% terms in Entries.
%% @end
%%--------------------------------------------------------------------
-spec make_mod(Mod :: module(),
	       Entries :: [{Key :: string(), Value :: term()}]) ->
    term().
make_mod(Mod, Entries) ->
    ModuleName = cerl:c_atom(Mod),
    cerl:c_module(ModuleName,
		  [cerl:c_fname(entries, 0),
		   cerl:c_fname(lookup, 1),
		   cerl:c_fname(module_info, 0),
		   cerl:c_fname(module_info, 1)],
		  [make_entries_fun(Entries),
		   make_lookup_fun(Entries) | mod_info(ModuleName)]).

%%--------------------------------------------------------------------
%% @doc
%% Make entries/0 function.
%% @end
%%--------------------------------------------------------------------
make_entries_fun(Entries) ->
    {cerl:c_fname(entries,0), cerl:c_fun([], cerl:abstract(Entries))}.

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

%% Taken from cerl.erl and added maps support.
%% @spec is_literal_term(Term::term()) -> boolean()
%%
%% @doc Returns <code>true</code> if <code>Term</code> can be
%% represented as a literal, otherwise <code>false</code>. This
%% function takes time proportional to the size of <code>Term</code>.
%% This function is a copy from cerl.erl with added support for
%% maps.
%% @see abstract/1

-spec is_literal_term(term()) -> boolean().

is_literal_term(T) when is_integer(T) -> true;
is_literal_term(T) when is_float(T) -> true;
is_literal_term(T) when is_atom(T) -> true;
is_literal_term([]) -> true;
is_literal_term([H | T]) ->
    is_literal_term(H) andalso is_literal_term(T);
is_literal_term(T) when is_tuple(T) ->
    is_literal_term_list(tuple_to_list(T));
is_literal_term(T) when is_map(T) ->
    is_literal_term_list(maps:to_list(T));
is_literal_term(B) when is_bitstring(B) -> true;
is_literal_term(_) ->
    false.

-spec is_literal_term_list([term()]) -> boolean().

is_literal_term_list([T | Ts]) ->
    case is_literal_term(T) of
	true ->
	    is_literal_term_list(Ts);
	false ->
	    false
    end;
is_literal_term_list([]) ->
    true.

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
