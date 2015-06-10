%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%% GB_HASH Applicarion server that initiates hash rings.
%%% @end
%%% Created :  18 Mar 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash_server).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([register_func/2,
	 unregister_func/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-include("gb_hash.hrl").
-include("gb_log.hrl").

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Register a hash ring function by writing a #gb_hash_register{}
%% record to mnesia dets table.
%% @end
%%--------------------------------------------------------------------
-spec register_func(Name :: string(), Func :: #gb_hash_func{}) ->
     ok | {error, Reason :: term()}.
register_func(Name, Func) ->
    Transaction =
        fun() ->
	    mnesia:write(#gb_hash_register{name = Name, func = Func})
	end,
    case gb_hash_db:transaction(Transaction) of
	{atomic, ok} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Unregister a hash ring function by deleting a record specified by
%% Name as key from mnesia dets table gb_hash_func.
%% @end
%%--------------------------------------------------------------------
-spec unregister_func(Name :: string()) ->
    ok | {error, Reason :: term()}.
unregister_func(Name) ->
    Transaction =
        fun() ->
	    mnesia:delete(gb_hash_register, Name, write)
	end,
    case gb_hash_db:transaction(Transaction) of
	{atomic, ok} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.


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
    ok = initiate_register(),
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
-spec initiate_register() -> ok | {error, Reason :: term()}.
initiate_register() ->
    Transaction =
	fun() ->
	    mnesia:foldl(fun fold_register/2, ok, gb_hash_register)
	end,
    case gb_hash_db:transaction(Transaction) of
	{atomic, ok} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec fold_register(Register :: #gb_hash_register{}, ok) -> ok | {error, Acc :: term()}.
fold_register(#gb_hash_register{name=Name, func=Func}, ok) ->
    case mochiglobal:get(list_to_atom(Name)) of
	undefined ->
	    mochiglobal:put(list_to_atom(Name), Func);
	Func ->
	    ok
    end;
fold_register(#gb_hash_register{} , Acc) ->
    {error, Acc}.

