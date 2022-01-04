%% ==========================================================================================================
%% Ram - An in-memory distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2021 Roberto Ostinelli <roberto@ostinelli.net>.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.
%% ==========================================================================================================
-module(ram_benchmark).

%% API
-export([
    start/0,
    put_on_node/4,
    get_on_node/4
]).
-export([
    start_profiling/1,
    stop_profiling/1,
    start_profiling_on_node/0,
    stop_profiling_on_node/0
]).

%% ===================================================================
%% API
%% ===================================================================

%% example run: `KEY_COUNT=100000 WORKERS_PER_NODE=100 NODES_COUNT=2 make bench`
start() ->
    %% init
    KeyCount = list_to_integer(os:getenv("KEY_COUNT", "100000")),
    WorkersPerNode = list_to_integer(os:getenv("WORKERS_PER_NODE", "100")),
    SlavesCount = list_to_integer(os:getenv("NODES_COUNT", "2")),

    KeysPerNode = round(KeyCount / SlavesCount),
    MaxKeyCount = KeysPerNode * SlavesCount,

    io:format("====> Starting benchmark~n"),
    io:format("      --> Nodes: ~w / ~w slave(s)~n", [SlavesCount + 1, SlavesCount]),
    io:format("      --> Total keys: ~w (~w / slave node)~n", [MaxKeyCount, KeysPerNode]),
    io:format("      --> Workers per node: ~w~n~n", [WorkersPerNode]),

    %% start nodes
    NodesInfo = lists:foldl(fun(I, Acc) ->
        %% start slave
        CountBin = integer_to_binary(I),
        NodeShortName = list_to_atom(binary_to_list(<<"slave_", CountBin/binary>>)),
        {ok, Node} = ct_slave:start(NodeShortName, [
            {boot_timeout, 10},
            {monitor_master, true}
        ]),
        %% add code path
        CodePath = code:get_path(),
        true = rpc:call(Node, code, set_path, [CodePath]),
        %% start ram
        ok = rpc:call(Node, ram, start, []),
        %% gather data
        FromKey = (I - 1) * KeysPerNode + 1,
        ToKey = FromKey + KeysPerNode - 1,
        %% fold
        [{Node, FromKey, ToKey} | Acc]
    end, [], lists:seq(1, SlavesCount)),

    %% start ram locally
    ok = ram:start(),
    timer:sleep(1000),

    CollectorPid = self(),

    io:format("~n====> Starting benchmark~n~n"),

    %% start registration
    lists:foreach(fun({Node, FromKey, ToKey}) ->
        rpc:cast(Node, ?MODULE, put_on_node, [CollectorPid, WorkersPerNode, FromKey, ToKey])
    end, NodesInfo),

    %% wait
    PutRemoteNodesTimes = wait_from_all_remote_nodes(nodes(), []),

    %% ckeck
    MaxKeyCount = ram:get(MaxKeyCount),

    io:format("----> Remote put times:~n"),
    io:format("      --> MIN: ~p secs.~n", [lists:min(PutRemoteNodesTimes)]),
    io:format("      --> MAX: ~p secs.~n", [lists:max(PutRemoteNodesTimes)]),

    PutRate = MaxKeyCount / lists:max(PutRemoteNodesTimes),
    io:format("====> Put rate: ~p/sec.~n~n", [PutRate]),

    %% start reading
    lists:foreach(fun({Node, FromKey, ToKey}) ->
        rpc:cast(Node, ?MODULE, get_on_node, [CollectorPid, WorkersPerNode, FromKey, ToKey])
    end, NodesInfo),

    %% wait
    GetRemoteNodesTimes = wait_from_all_remote_nodes(nodes(), []),

    io:format("----> Remote get times:~n"),
    io:format("      --> MIN: ~p secs.~n", [lists:min(GetRemoteNodesTimes)]),
    io:format("      --> MAX: ~p secs.~n", [lists:max(GetRemoteNodesTimes)]),

    GetRate = MaxKeyCount / lists:max(GetRemoteNodesTimes),
    io:format("====> Get rate: ~p/sec.~n~n", [GetRate]),

    %% stop node
    init:stop().

put_on_node(CollectorPid, WorkersPerNode, FromKey, ToKey) ->
    Count = ToKey - FromKey + 1,
    %% spawn workers
    KeysPerNode = ceil(Count / WorkersPerNode),
    ReplyPid = self(),
    lists:foreach(fun(I) ->
        WorkerFromKey = FromKey + (I - 1) * KeysPerNode,
        WorkerToKey = lists:min([WorkerFromKey + KeysPerNode - 1, ToKey]),
        spawn(fun() ->
            StartAt = os:system_time(millisecond),
            worker_put_on_node(WorkerFromKey, WorkerToKey),
            Time = (os:system_time(millisecond) - StartAt) / 1000,
            ReplyPid ! {done, Time}
        end)
    end, lists:seq(1, WorkersPerNode)),
    %% wait
    Time = wait_done_on_node(CollectorPid, 0, WorkersPerNode),
    io:format("----> Put on node ~p on ~p secs.~n", [node(), Time]).

get_on_node(CollectorPid, WorkersPerNode, FromKey, ToKey) ->
    Count = ToKey - FromKey + 1,
    %% spawn workers
    KeysPerNode = ceil(Count / WorkersPerNode),
    ReplyPid = self(),
    lists:foreach(fun(I) ->
        WorkerFromKey = FromKey + (I - 1) * KeysPerNode,
        WorkerToKey = lists:min([WorkerFromKey + KeysPerNode - 1, ToKey]),
        spawn(fun() ->
            StartAt = os:system_time(millisecond),
            worker_get_on_node(WorkerFromKey, WorkerToKey),
            Time = (os:system_time(millisecond) - StartAt) / 1000,
            ReplyPid ! {done, Time}
        end)
    end, lists:seq(1, WorkersPerNode)),
    %% wait
    Time = wait_done_on_node(CollectorPid, 0, WorkersPerNode),
    io:format("----> Put on node ~p on ~p secs.~n", [node(), Time]).

worker_put_on_node(Key, WorkerToKey) when Key =< WorkerToKey ->
    ok = ram:put(Key, Key),
    worker_put_on_node(Key + 1, WorkerToKey);
worker_put_on_node(_, _) -> ok.

worker_get_on_node(Key, WorkerToKey) when Key =< WorkerToKey ->
    Key = ram:get(Key),
    worker_get_on_node(Key + 1, WorkerToKey);
worker_get_on_node(_, _) -> ok.

wait_done_on_node(CollectorPid, Time, 0) ->
    CollectorPid ! {done, node(), Time},
    Time;
wait_done_on_node(CollectorPid, Time, WorkersRemainingCount) ->
    receive
        {done, WorkerTime} ->
            Time1 = lists:max([WorkerTime, Time]),
            wait_done_on_node(CollectorPid, Time1, WorkersRemainingCount - 1)
    end.

wait_from_all_remote_nodes([], Times) -> Times;
wait_from_all_remote_nodes([RemoteNode | Tail], Times) ->
    receive
        {done, RemoteNode, Time} ->
            wait_from_all_remote_nodes(Tail, [Time | Times])
    end.

start_profiling(NodesInfo) ->
    {Node, _FromKey, _ToKey} = hd(NodesInfo),
    ok = rpc:call(Node, ?MODULE, start_profiling_on_node, []).

stop_profiling(NodesInfo) ->
    {Node, _FromKey, _ToKey} = hd(NodesInfo),
    ok = rpc:call(Node, ?MODULE, stop_profiling_on_node, []).

start_profiling_on_node() ->
    {ok, P} = eprof:start(),
    eprof:start_profiling(erlang:processes() -- [P]),
    ok.

stop_profiling_on_node() ->
    eprof:stop_profiling(),
    eprof:analyze(total),
    ok.
