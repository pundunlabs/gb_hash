%%%-------------------------------------------------------------------
%%% @author erdem aksu <erdem@sitting>
%%% @copyright (C) 2015, Mobile Arts AB
%%% @doc
%%% Growbeard Hash Application that provides hash utilities.
%%% @end
%%% Created :  17 Feb 2015 by erdem <erdem@sitting>
%%%-------------------------------------------------------------------
-module(gb_hash).

 
%% API
-export([create_ring/2,
         create_ring/3,
         find_node/2,
	 get_nodes/1,
         test/1]).

-include("gb_hash.hrl").

-type hash_algorithms() ::  md5 | ripemd160 | 
                            sha | sha224 | sha256 |
                            sha384 | sha512.

-type hash_strategy() :: consistent | uniform.

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
%%--------------------------------------------------------------------
-spec create_ring(Name::string(), Nodes::[term()])-> ok.
create_ring(Name, Nodes) ->
    create_ring(Name, Nodes, []).

%%--------------------------------------------------------------------
%% @doc
%% Create a ring of nodes where each node gets an assigned hash value.
%% Set hash algorithm and strategy by prooviding Options.
%%--------------------------------------------------------------------
-spec create_ring(Name::string(), Nodes::[term()], Options:: [option()])-> ok.
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
    ok = gb_hash_server:register_func(Name, HashFunc),
    mochiglobal:put(list_to_atom(Name), HashFunc).

%%--------------------------------------------------------------------
%% @doc
%% Find node for a given key on Names' hash ring.
%%--------------------------------------------------------------------
-spec find_node(Name::string(), Key::term())-> {ok, Node::term()} | undefined.
find_node(Name, Key) ->
    case mochiglobal:get(list_to_atom(Name)) of
        undefined ->
            undefined;
        #gb_hash_func{type = Type,
                      ring = Ring}->
            find_node(Ring, Type, Key)
    end.

-spec find_node(Ring::[{Hash::binary(), Node::term()}],
                Type::hash_algorithms(),
                Key::term()) -> {ok, Node::term()}.
find_node(Ring, Type, Key) ->
    Hash = hash(Type, Key),
    Node = find_near_hash(Ring, Hash),
    {ok, Node}.

-spec find_near_hash(Ring::[{Hash::binary(), Node::term()}],
                     Hash::binary()) -> Node::term().
find_near_hash(Ring, Hash) ->
    find_near_hash(Ring, Hash, hd(Ring)).

-spec find_near_hash(Ring::[{H::binary(), Node::term()}], 
                     Hash::binary(),
                     First::{H::binary(), Node::term()}) -> Node::term().
find_near_hash([{H, Node}|_], Hash, _) when Hash < H ->
    Node;
find_near_hash([_|T], Hash, First) ->
    find_near_hash(T, Hash, First);
find_near_hash([], _, {_,Node}) ->
    Node.

%%--------------------------------------------------------------------
%% @doc
%% Get all nodes for a given key on Names' hash ring.
%%--------------------------------------------------------------------
-spec get_nodes(Name::string())-> {ok, [Node::term()]} | undefined.
get_nodes(Name) ->
    case mochiglobal:get(list_to_atom(Name)) of
        undefined ->
            undefined;
        #gb_hash_func{ring = Ring}->
            {ok, [ Node || {_H, Node} <- Ring ]}
    end.

-spec get_consistent_ring(Algo :: hash_algorithms(), Nodes::[term()]) ->
    {ok, Ring::[{Hash::binary(), Node::term()}]}.
get_consistent_ring(Algo, Nodes)->
    {ok, lists:keysort(1, [{hash(Algo, Node), Node} || Node <- Nodes])}.

-spec get_uniform_ring(Algo :: hash_algorithms(), Nodes::[term()]) ->
    {ok, Ring::[{Hash::binary, Node::term()}]}.
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

-spec hash(Type::hash_algorithms(), Data::binary())-> Digest::binary()
        ; (Type::hash_algorithms(), Data::term()) -> Digest::binary().
hash(Type, Data) when is_binary(Data)->
    crypto:bytes_to_integer(crypto:hash(Type, Data));
hash(Type, Data) ->
    hash(Type, term_to_binary(Data)).


%%--------------------------------------------------------------------
%% @doc
%% Returns the number of keys distributed each shard of a given Table.
%% Number of keys are hard coded to 100000.
%%--------------------------------------------------------------------
-spec test(Table::string()) -> [{Shard::term(), Count::integer()}].
test(Table) ->
    random:seed(now()),
    List = [begin
                {ok, Shard} = find_node(Table, random:uniform(10000000)),
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

