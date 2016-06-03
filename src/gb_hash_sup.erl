%%%-------------------------------------------------------------------
%%% @author erdem <erdem@sitting>
%%% @copyright (C) 2015, erdem
%%% @doc
%%%
%%% @end
%%% Created :  18 Mar 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("gb_hash.hrl").

-define(SERVER, ?MODULE).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% 
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
		      ignore |
		      {error, Error :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    LocOpts = [{name, ?LocReg},
	       {mod, ?LocMod},
	       {file, "/data/gb_hash_loc_register"}],
    GBHLocRegister = {?LocReg,
		      {gb_hash_register, start_link, [LocOpts]},
		      permanent, 2000, worker, [gb_hash_register]},

    DistOpts = [{name, ?DistReg},
	       {mod, ?DistMod},
	       {file, "/data/gb_hash_dist_register"}],
    GBHDistRegister = {?DistReg,
		       {gb_hash_register, start_link, [DistOpts]},
		       permanent, 2000, worker, [gb_hash_register]},

    {ok, {SupFlags, [GBHLocRegister, GBHDistRegister]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

