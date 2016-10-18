%%%-------------------------------------------------------------------
%%% @author erdem aksu <erdem@sitting>
%%% @copyright (C) 2015, Pundun Labs
%%% @doc
%%% Growbeard Hash Application that provides hash utilities.
%%% @end
%%% Created :  17 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash).

%% API
-export([create_ring/2,
         create_ring/3,
         delete_ring/1,
	 get_node/2,
	 get_local_node/2,
	 get_nodes/1,
	 hash/2,
	 exists/1,
	 is_distributed/1,
	 all_entries/0]).

-include("gb_hash.hrl").

-type hash_algorithms() ::  md5 | ripemd160 | 
                            sha | sha224 | sha256 |
                            sha384 | sha512 |
			    wrapper().

-type hash_strategy() :: consistent | uniform | timedivision.

-type wrapper() :: {tda,
		    FileMargin :: pos_integer(),
		    TimeMargin :: pos_integer()}.

-type option() :: [{algorithm, hash_algorithms()} |
                   {strategy, hash_strategy()}].

%%-type timestamp() :: {pos_integer(), pos_integer(), pos_integer()}.

%% trunc(math:pow(2,160) -1)

-define(MAX_MD5, 340282366920938463463374607431768211456).
-define(MAX_SHA, 1461501637330902918203684832716283019655932542976).
-define(MAX_SHA224, 26959946667150639794667015087019630673637144422540572481103610249216).
-define(MAX_SHA256, 115792089237316195423570985008687907853269984665640564039457584007913129639936).
-define(MAX_SHA384, 39402006196394479212279040100143613805079739270465446667948293404245721771497210611414266254884915640806627990306816).
-define(MAX_SHA512, 13407807929942597099574024998205846127479365820592393377723561443721764030073546976801874298166903427690031858186486050853753882811946569946433649006084096).

%%%===================================================================
%%% API
%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create a ring of nodes where each node gets an assigned hash value.
%% @end
%%--------------------------------------------------------------------
-spec create_ring(Name :: string(), Nodes :: [term()])-> ok.
create_ring(Name, Nodes) ->
    create_ring(Name, Nodes, []).

%%--------------------------------------------------------------------
%% @doc
%% Create a ring of nodes where each node gets an assigned hash value.
%% Set hash algorithm and strategy by prooviding Options.
%% @end
%%--------------------------------------------------------------------
-spec create_ring(Name :: string(),
		  Nodes :: [term()],
		  Options :: [option()])->
    {ok, Beam :: binary()} | {error, Reason :: term()}.
create_ring(Name, Nodes, Options) ->
    Algo = proplists:get_value(algorithm, Options, sha),
    Stra = proplists:get_value(strategy, Options, consistent),
    {ok, Ring} =
        case Stra of
            consistent ->
                get_consistent_ring(Algo, Nodes);
            uniform ->
                get_uniform_ring(Algo, Nodes)
        end,
    HashFunc = #gb_hash_func{type = Algo,
                             ring = Ring},
    Reg = case proplists:get_value(local, Options) of
	    true -> ?LocReg;
	    _ -> ?DistReg
	  end,
    gb_hash_register:insert(Reg, Name, HashFunc).

%%--------------------------------------------------------------------
%% @doc
%% Delete a hash ring.
%% @end
%%--------------------------------------------------------------------
-spec delete_ring(Name :: string())-> ok.
delete_ring(Name) ->
    Reg = where_is(Name),
    case gb_hash_register:delete(Reg, Name) of
	{ok, _Bin} ->
	    ok;
	Else ->
	    Else
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get node for a given key on hash rings.
%%--------------------------------------------------------------------
-spec get_node(Name :: string(), Key :: term())->
    {ok, Node :: term()} | undefined.
get_node(Name, Key) ->
    case get_node(?DistMod, Name, Key) of
	undefined ->
	    get_node(?LocMod, Name, Key);
	R ->
	    R
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get node for a given key on local hash ring.
%%--------------------------------------------------------------------
-spec get_local_node(Name :: string(), Key :: term())->
    {ok, Node :: term()} | undefined.
get_local_node(Name, Key) ->
    get_node(?LocMod, Name, Key).

%%--------------------------------------------------------------------
%% @doc
%% Get node for a given key on Mod module's hash ring.
%%--------------------------------------------------------------------
-spec get_node(Mod :: atom(), Name :: string(), Key :: term())->
    {ok, Node :: term()} | undefined.
get_node(Mod, Name, Key) ->
    case gb_hash_register:lookup(Mod, Name) of
        undefined ->
            undefined;
        #gb_hash_func{type = Type,
                      ring = Ring} ->
            find_node(Ring, Type, Key)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get all nodes for a given key on hash rings.
%% @end
%%--------------------------------------------------------------------
-spec get_nodes(Name :: string())-> {ok, [Node :: term()]} | undefined.
get_nodes(Name) ->
    case get_nodes(?DistMod, Name) of
        undefined ->
            get_nodes(?LocMod, Name);
        R ->
            R
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get all nodes for a given key on Mod module's hash ring.
%% @end
%%--------------------------------------------------------------------
-spec get_nodes(Mod :: atom(),
		Name :: string())->
    {ok, [Node :: term()]} | undefined.
get_nodes(Mod, Name) ->
    case gb_hash_register:lookup(Mod, Name) of
        undefined ->
            undefined;
        #gb_hash_func{ring = Ring}->
            {ok, [ Node || {_H, Node} <- Ring ]}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if table with Name exists on hash rings.
%% @end
%%--------------------------------------------------------------------
-spec exists(Name :: string())->
    true | false.
exists(Name) ->
    case gb_hash_register:lookup(?DistMod, Name) of
        undefined ->
	    case gb_hash_register:lookup(?LocMod, Name) of
		undefined ->
		    false;
		_ -> true
	    end;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if the table with given Name is distributed.
%% @end
%%--------------------------------------------------------------------
-spec is_distributed(Name :: string())->
    Bool :: boolean().
is_distributed(Name) ->
    case gb_hash_register:lookup(?DistMod, Name) of
        undefined ->
	    case gb_hash_register:lookup(?LocMod, Name) of
		undefined ->
		    undefined;
		_ -> false
	    end;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Return all entries for.
%% @end
%%--------------------------------------------------------------------
-spec all_entries()->
    Entries :: [term()].
all_entries() ->
    Entries  = gb_hash_register:all(?DistMod),
    [Name || {Name,_} <- Entries].

%%--------------------------------------------------------------------
%% @doc
%% Check on which hash ring the table with Name exists.
%% @end
%%--------------------------------------------------------------------
-spec where_is(Name :: string())->
    Reg ::atom().
where_is(Name) ->
    case gb_hash_register:lookup(?DistMod, Name) of
        undefined ->
	    case gb_hash_register:lookup(?LocMod, Name) of
		undefined ->
		    undefined;
		_ -> ?LocReg
	    end;
        _ -> ?DistReg
    end.

-spec find_node(Ring :: [{Hash :: binary(), Node :: term()}],
                Type :: hash_algorithms(),
                Key :: term()) -> {ok, {level, Node :: term()}} |
				  {ok, Node :: term()} |
				  undefined.
find_node(Ring, Type, Key) ->
    Hash = hash(Type, Key),
    Node = find_near_hash(Ring, Hash),
    {ok, Node}.

-spec find_near_hash(Ring :: [{Hash :: binary(), Node :: term()}],
                     Hash :: binary()) ->
    Node :: term().
find_near_hash(Ring, Hash) ->
    find_near_hash(Ring, Hash, hd(Ring)).

-spec find_near_hash(Ring :: [{H :: binary(), Node :: term()}], 
                     Hash :: binary(),
                     First :: {H :: binary(), Node :: term()}) ->
    Node :: term().
find_near_hash([{H, Node}|_], Hash, _) when Hash < H ->
    Node;
find_near_hash([_|T], Hash, First) ->
    find_near_hash(T, Hash, First);
find_near_hash([], _, {_,Node}) ->
    Node.

-spec get_consistent_ring(Algo :: hash_algorithms(), Nodes :: [term()]) ->
    {ok, Ring :: [{Hash :: binary(), Node :: term()}]}.
get_consistent_ring(Algo, Nodes)->
    {ok, lists:keysort(1, [{hash(Algo, Node), Node} || Node <- Nodes])}.

-spec get_uniform_ring(Algo :: hash_algorithms(), Nodes :: [term()]) ->
    {ok, Ring :: [{Hash :: binary, Node :: term()}]}.
get_uniform_ring(Algo, Nodes) ->
    Num = length(Nodes),
    MaxHash =
        case Algo of
            md5 -> ?MAX_MD5;
            ripemd160  -> ?MAX_SHA;
            sha -> ?MAX_SHA;
            sha224 -> ?MAX_SHA224;
            sha256 -> ?MAX_SHA256;
            sha384 -> ?MAX_SHA384;
            sha512 -> ?MAX_SHA512
        end,
    Range = MaxHash div Num,
    %%Ranges = [binary:encode_unsigned(N*Range) || N <- lists:seq(1,Num)],
    Ranges = [N*Range || N <- lists:seq(1,Num)],
    Ring = lists:zip(Ranges, Nodes),
    {ok, Ring}.

%%-spec get_time_division_ring(FileMargin :: pos_integer(),
%%			     Nodes :: [term()]) ->
%%    {ok, Ring :: [{Ts :: timestamp(), Node :: term()}]}.
%%get_time_division_ring(FileMargin, Nodes) ->
%%    Modulus = lists:seq(0, FileMargin-1),
%%    Ring = lists:zip(Modulus, Nodes),
%%    {ok, Ring}.

-spec hash(Type :: hash_algorithms(), Data :: binary())-> Digest :: binary()
        ; (Type :: hash_algorithms(), Data :: term()) -> Digest :: binary().
hash(Type, Data) when is_binary(Data)->
    crypto:bytes_to_integer(crypto:hash(Type, Data));
hash(Type, Data) ->
    hash(Type, term_to_binary(Data)).
