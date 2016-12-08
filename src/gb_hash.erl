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
                            sha384 | sha512.

-type hash_strategy() :: consistent | uniform | rendezvous | virtual_nodes.

-type option() :: [{algorithm, hash_algorithms()} |
                   {strategy, hash_strategy()}].

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
    Algo = proplists:get_value(algorithm, Options, sha256),
    Stra = proplists:get_value(strategy, Options, consistent),
    HashFunc =
        case Stra of
            consistent ->
                {ok, Ring} = get_consistent_ring(Algo, Nodes),
		#{strategy => Stra, algorithm => Algo, ring => Ring};
            uniform ->
                {ok, Ring} = get_uniform_ring(Algo, Nodes),
		#{strategy => Stra, algorithm => Algo, ring => Ring};
            virtual_nodes ->
		VNodes = proplists:get_value(vnodes, Options, 512),
                {ok, Ring} = get_vnodes_ring(Algo, Nodes, VNodes),
		#{strategy => Stra, algorithm => Algo, ring => Ring};
	    rendezvous ->
		{ok, Ring, Skeleton} = get_rendezvous_ring(Nodes),
		#{strategy => Stra, algorithm => Algo, ring => Ring,
		  skeleton => Skeleton}
	end,
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
        #{strategy := rendezvous, algorithm := Algo, ring := Ring,
	  skeleton := Skeleton} ->
            find_fhrw_node(Skeleton, Ring, Algo, Key);
        #{algorithm := Algo, ring := Ring} ->
            find_node(Ring, Algo, Key)
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
        #{strategy:= rendezvous, ring := Ring}->
	    {ok, Ring};
	#{ring := Ring}->
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
		undefined -> false;
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
                Algo :: hash_algorithms(),
                Key :: term()) ->
    {ok, Node :: term()} | undefined.
find_node(Ring, Algo, Key) ->
    Hash = hash(Algo, Key),
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

-spec find_fhrw_node(Skeleton :: [[integer()]],
		     Ring :: term(),
		     Algo :: hash_algorithms(),
		     Key :: term()) ->
    {ok, Node :: term()} | undefined.
find_fhrw_node(Skeleton, Ring, Algo, Key) ->
    Path = fanout(Skeleton, Algo, Key, 0, []),
    {ok, maps:get(Path, Ring)}.

-spec fanout(Skeleton :: [[integer()]],
	     Algo :: hash_algorithms(),
	     Key :: term(),
	     S :: integer(),
	     Acc :: [integer()]) ->
    Path :: [pos_integer()].
fanout([H | T], Algo, Key, S, Acc) ->
    Digit = find_hrw_node(H, Algo, {S, Key}),
    fanout(T, Algo, Key, Digit, [Digit | Acc]);
fanout([], _Algo, _Key, _, Acc) ->
    lists:reverse(Acc).

find_hrw_node([N | T], Algo, Key) ->
    find_hrw_node(T, Algo, Key, {N, hash(Algo, {N, Key})}).

find_hrw_node([H | T], Algo, Key, {N, Max}) ->
    case hash(Algo, {H, Key}) of
	Hash when Hash > Max ->
	    find_hrw_node(T, Algo, Key, {H, Hash});
	_ ->
	    find_hrw_node(T, Algo, Key, {N, Max})
    end;
find_hrw_node([], _, _,{Node, _}) ->
    Node.

-spec get_consistent_ring(Algo :: hash_algorithms(), Nodes :: [term()]) ->
    {ok, Ring :: [{Hash :: binary(), Node :: term()}]}.
get_consistent_ring(Algo, Nodes)->
    {ok, lists:keysort(1, [{hash(Algo, Node), Node} || Node <- Nodes])}.

-spec get_uniform_ring(Algo :: hash_algorithms(), Nodes :: [term()]) ->
    {ok, Ring :: [{Hash :: binary(), Node :: term()}]}.
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

-spec get_vnodes_ring(Algo :: hash_algorithms(),
		      Nodes :: term(),
		      VNodes :: pos_integer()) ->
    {ok, Ring :: [{Hash :: binary(), Node :: term()}]}.
get_vnodes_ring(Algo, Nodes, VNodes) ->
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
    Range = MaxHash div VNodes,
    Ring = [{N*Range, Node} || {N, Node} <- map_vnodes(VNodes, Nodes)],
    {ok, Ring}.

-spec get_rendezvous_ring(Nodes :: [term()]) ->
    {ok, Ring :: map(), Skeleton :: [[integer()]]}.
get_rendezvous_ring(Nodes) ->
    Skeleton = skeleton(length(Nodes)),
    Cartesian = cartesian(Skeleton),
    List = lists:zip(Cartesian, Nodes),
    Ring = maps:from_list(List),
    {ok, Ring, Skeleton}.

-spec map_vnodes(VNodes :: pos_integer(),
		 Nodes :: [term()]) ->
    map().
map_vnodes(VNodes, Nodes) ->
    map_vnodes(VNodes, Nodes, []).

-spec map_vnodes(VNodes :: integer(),
		 Nodes :: [term()],
		 Mapping :: [Node :: term()]) ->
    [term()].
map_vnodes(0, _, Mapping) ->
    Mapping;
map_vnodes(VNodes, [N | Nodes], Mapping) ->
    map_vnodes(VNodes - 1, lists:append(Nodes, [N]), [{VNodes, N} | Mapping]).

-spec hash(Type :: hash_algorithms(), Data :: binary())-> Digest :: binary()
        ; (Type :: hash_algorithms(), Data :: term()) -> Digest :: binary().
hash(Type, Data) when is_binary(Data)->
    crypto:bytes_to_integer(crypto:hash(Type, Data));
hash(Type, Data) ->
    hash(Type, term_to_binary(Data)).

-spec factorize(N :: pos_integer()) ->
    [pos_integer()].
factorize(1) ->
    [1];
factorize(N) ->
    factorize(N, 2, []).

-spec factorize(N :: pos_integer(),
		F :: pos_integer(),
		Acc :: [pos_integer()]) ->
    [pos_integer()].
factorize(N, N, Acc)->
    lists:reverse([N | Acc]);
factorize(N, F, Acc) when N rem F == 0 ->
    factorize(N div F, F, [F | Acc]);
factorize(N, F, Acc) ->
    factorize(N, F+1, Acc).

-spec skeleton(Int :: pos_integer()) ->
    [[pos_integer()]].
skeleton(N) ->
    skeleton(factorize(N), []).

-spec skeleton(Factors :: [pos_integer()],
	       Acc :: [pos_integer()]) ->
    [[pos_integer()]].
skeleton([2, N | Rest], Acc) ->
    skeleton(Rest, [2*N|Acc]);
skeleton([N | Rest], Acc) ->
    skeleton(Rest, [N|Acc]);
skeleton([], Acc) ->
    lists:reverse([ lists:seq(0, I-1) || I <- Acc]).

-spec cartesian([[term()]]) ->
    [term()].
cartesian([H])   ->
    [[A] || A <- H];
cartesian([H|T]) ->
    [[A|B] || A <- H, B <- cartesian(T)].
