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
-module(ram_SUITE).

%% callbacks
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([groups/0, init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    one_node_operations/1
]).
-export([
    two_nodes_only_one_active/1
]).
-export([
    three_nodes_discover/1,
    three_nodes_operations/1,
    three_nodes_cluster_changes/1,
    three_nodes_transaction_fail/1,
    three_nodes_conflicts_default_resolution/1
]).
-export([
    five_nodes_stress_test/1
]).

%% include
-include_lib("common_test/include/ct.hrl").
-include("../src/ram.hrl").

%% ===================================================================
%% Callbacks
%% ===================================================================

%% -------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = any()
%% -------------------------------------------------------------------
all() ->
    [
        {group, one_node},
        {group, two_nodes},
        {group, three_nodes},
        {group, five_nodes}
    ].

%% -------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName =  atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%			   repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% -------------------------------------------------------------------
groups() ->
    [
        {one_node, [shuffle], [
            one_node_operations
        ]},
        {two_nodes, [shuffle], [
            two_nodes_only_one_active
        ]},
        {three_nodes, [shuffle], [
            three_nodes_discover,
            three_nodes_operations,
            three_nodes_cluster_changes,
            three_nodes_transaction_fail,
            three_nodes_conflicts_default_resolution
        ]},
        {five_nodes, [shuffle], [
            five_nodes_stress_test
        ]}
    ].
%% -------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%				Config1 | {skip,Reason} |
%%              {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    Config.

%% -------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% -------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%% -------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%				Config1 | {skip,Reason} |
%%              {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_group(two_nodes, Config) ->
    do_init_per_group(two_nodes, 2, Config);
init_per_group(three_nodes, Config) ->
    do_init_per_group(three_nodes, 3, Config);
init_per_group(five_nodes, Config) ->
    do_init_per_group(five_nodes, 5, Config);
init_per_group(_GroupName, Config) ->
    Config.

do_init_per_group(GroupName, Count, Config) ->
    case ram_test_suite_helper:init_cluster(Count) of
        {error_initializing_cluster, Other} ->
            end_per_group(GroupName, Config),
            {skip, Other};

        NodesConfig ->
            NodesConfig ++ Config
    end.

%% -------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%				void() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% -------------------------------------------------------------------
end_per_group(two_nodes, Config) ->
    ram_test_suite_helper:end_cluster(2, Config);
end_per_group(three_nodes, Config) ->
    ram_test_suite_helper:end_cluster(3, Config);
end_per_group(five_nodes, Config) ->
    ram_test_suite_helper:end_cluster(5, Config);
end_per_group(_GroupName, _Config) ->
    ram_test_suite_helper:clean_after_test().

%% -------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%				Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test: ~p", [TestCase]),
    Config.

%% -------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%				void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
end_per_testcase(_, _Config) ->
    ram_test_suite_helper:clean_after_test().

%% ===================================================================
%% Tests
%% ===================================================================
one_node_operations(_Config) ->
    %% start ram
    ok = ram:start(),

    %% check
    undefined = ram:get("key"),

    %% put
    ram:put("key", "value"),
    "value" = ram:get("key"),

    %% delete
    ok = ram:delete("key"),
    undefined = ram:get("key").

two_nodes_only_one_active(_Config) ->
    %% start ram only on main
    ok = ram:start(),

    %% check
    undefined = ram:get("key"),

    %% put
    ram:put("key", "value"),
    "value" = ram:get("key"),

    %% delete
    ok = ram:delete("key"),
    undefined = ram:get("key").

three_nodes_discover(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% start ram
    ok = ram:start(),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), []),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, undefined),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, undefined),

    %% start ram on 1
    ok = rpc:call(SlaveNode1, ram, start, []),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, undefined),

    %% start ram on 2
    ok = rpc:call(SlaveNode2, ram, start, []),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% disconnect node 1 from 2
    rpc:call(SlaveNode1, ram_test_suite_helper, disconnect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node()]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]),

    %% disconnect master from 1
    ram_test_suite_helper:disconnect_node(SlaveNode1),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node()]),
    %% NB: can't check 1 since disconnected

    %% reconnect all
    ram_test_suite_helper:connect_node(SlaveNode1),
    rpc:call(SlaveNode1, ram_test_suite_helper, connect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% crash a kv process on 2
    rpc:call(SlaveNode2, ram_test_suite_helper, kill_process, [ram_kv]),
    rpc:call(SlaveNode2, ram_test_suite_helper, wait_process_name_ready, [ram_kv]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% kill ram on 1
    rpc:call(SlaveNode1, ram, stop, []),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, undefined),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]).

three_nodes_operations(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% start ram
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),

    %% check
    error = ram:fetch("key"),
    undefined = ram:get("key"),
    default = ram:get("key", default),
    undefined = rpc:call(SlaveNode1, ram, get, ["key"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key"]),

    %% put
    ram:put("key", "value"),

    %% check
    {ok, "value"} = ram:fetch("key"),
    "value" = ram:get("key"),
    "value" = ram:get("key", default),
    "value" = rpc:call(SlaveNode1, ram, get, ["key"]),
    "value" = rpc:call(SlaveNode2, ram, get, ["key"]),

    %% delete
    ok = rpc:call(SlaveNode1, ram, delete, ["key"]),
    ok = rpc:call(SlaveNode1, ram, delete, ["key"]),

    %% check
    error = ram:fetch("key"),
    undefined = ram:get("key"),
    default = ram:get("key", default),
    undefined = rpc:call(SlaveNode1, ram, get, ["key"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key"]),

    ComplexKey = {key, self()},

    %% update
    UpdateFun = fun(ExistingValue) -> ExistingValue * 2 end,
    ok = ram:update(ComplexKey, 10, UpdateFun),

    %% check
    10 = ram:get(ComplexKey),
    10 = rpc:call(SlaveNode1, ram, get, [ComplexKey]),
    10 = rpc:call(SlaveNode2, ram, get, [ComplexKey]),

    %% update
    ok = ram:update(ComplexKey, 10, UpdateFun),

    %% check
    20 = ram:get(ComplexKey),
    20 = rpc:call(SlaveNode1, ram, get, [ComplexKey]),
    20 = rpc:call(SlaveNode2, ram, get, [ComplexKey]).

three_nodes_cluster_changes(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% disconnect node 1 from 2
    rpc:call(SlaveNode1, ram_test_suite_helper, disconnect_node, [SlaveNode2]),

    %% start ram on nodes
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]),

    %% put
    ok = rpc:call(SlaveNode1, ram, put, ["key-on-1", "value-on-1"]),
    ok = rpc:call(SlaveNode2, ram, put, ["key-on-2", "value-on-2"]),

    %% check
    "value-on-1" = ram:get("key-on-1"),
    "value-on-2" = ram:get("key-on-2"),
    "value-on-1" = rpc:call(SlaveNode1, ram, get, ["key-on-1"]),
    undefined = rpc:call(SlaveNode1, ram, get, ["key-on-2"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key-on-1"]),
    "value-on-2" = rpc:call(SlaveNode2, ram, get, ["key-on-2"]),

    %% reconnect all
    rpc:call(SlaveNode1, ram_test_suite_helper, connect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    "value-on-1" = ram:get("key-on-1"),
    "value-on-2" = ram:get("key-on-2"),
    "value-on-1" = rpc:call(SlaveNode1, ram, get, ["key-on-1"]),
    "value-on-2" = rpc:call(SlaveNode1, ram, get, ["key-on-2"]),
    "value-on-1" = rpc:call(SlaveNode2, ram, get, ["key-on-1"]),
    "value-on-2" = rpc:call(SlaveNode2, ram, get, ["key-on-2"]),

    %% disconnect node 1 from 2
    rpc:call(SlaveNode1, ram_test_suite_helper, disconnect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]),

    ok = rpc:call(SlaveNode1, ram, delete, ["key-on-1"]),
    ok = rpc:call(SlaveNode2, ram, delete, ["key-on-2"]),

    %% check
    undefined = ram:get("key-on-1"),
    undefined = ram:get("key-on-2"),
    undefined = rpc:call(SlaveNode1, ram, get, ["key-on-1"]),
    "value-on-2" = rpc:call(SlaveNode1, ram, get, ["key-on-2"]),
    "value-on-1" = rpc:call(SlaveNode2, ram, get, ["key-on-1"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key-on-2"]),

    %% reconnect all
    rpc:call(SlaveNode1, ram_test_suite_helper, connect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    undefined = ram:get("key-on-1"),
    undefined = ram:get("key-on-2"),
    undefined = rpc:call(SlaveNode1, ram, get, ["key-on-1"]),
    undefined = rpc:call(SlaveNode1, ram, get, ["key-on-2"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key-on-1"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key-on-2"]).

three_nodes_transaction_fail(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% start ram
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% put
    ok = ram:put("key-to-delete", "value-to-delete"),

    %% suspend ram on 1
    ok = rpc:call(SlaveNode1, sys, suspend, [ram_kv]),

    %% get transaction errors
    {'EXIT', {{commit_timeout, {bad_nodes, [SlaveNode1]}}, _}} = (catch ram:put("key", "value")),
    {'EXIT', {{commit_timeout, {bad_nodes, [SlaveNode1]}}, _}} = (catch ram:update(
        "key",
        "value",
        fun(ExistingValue) -> ExistingValue * 2 end)
    ),

    %% check
    undefined = ram:get("key"),
    undefined = rpc:call(SlaveNode1, ram, get, ["key"]),
    undefined = rpc:call(SlaveNode2, ram, get, ["key"]),

    %% delete with no previous key
    ok = ram:delete("key"),
    ok = rpc:call(SlaveNode1, ram, delete, ["key"]),
    ok = rpc:call(SlaveNode2, ram, delete, ["key"]),

    %% delete with existing previous key
    {'EXIT', {{commit_timeout, {bad_nodes, [SlaveNode1]}}, _}} = (catch ram:delete("key-to-delete")),

    %% check temp contents
    ram_test_suite_helper:assert_wait(
        [],
        fun() -> ets:tab2list(?TABLE_TRANSACTIONS) end
    ),
    ram_test_suite_helper:assert_wait(
        [],
        fun() -> rpc:call(SlaveNode1, ets, tab2list, [?TABLE_TRANSACTIONS]) end
    ),
    ram_test_suite_helper:assert_wait(
        [],
        fun() -> rpc:call(SlaveNode2, ets, tab2list, [?TABLE_TRANSACTIONS]) end
    ).

three_nodes_conflicts_default_resolution(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% disconnect node 1 from 2
    rpc:call(SlaveNode1, ram_test_suite_helper, disconnect_node, [SlaveNode2]),

    %% start ram on nodes
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]),

    %% put
    ok = rpc:call(SlaveNode1, ram, put, ["common-key", "value-on-1"]),
    ok = rpc:call(SlaveNode2, ram, put, ["common-key", "value-on-2"]),

    %% check
    "value-on-2" = ram:get("common-key"),
    "value-on-1" = rpc:call(SlaveNode1, ram, get, ["common-key"]),
    "value-on-2" = rpc:call(SlaveNode2, ram, get, ["common-key"]),

    %% reconnect all
    rpc:call(SlaveNode1, ram_test_suite_helper, connect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    "value-on-2" = ram:get("common-key"),
    "value-on-2" = rpc:call(SlaveNode1, ram, get, ["common-key"]),
    "value-on-2" = rpc:call(SlaveNode2, ram, get, ["common-key"]).

five_nodes_stress_test(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),
    SlaveNode3 = proplists:get_value(ram_slave_3, Config),
    SlaveNode4 = proplists:get_value(ram_slave_4, Config),

    %% start ram on nodes
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),
    ok = rpc:call(SlaveNode3, ram, start, []),
    ok = rpc:call(SlaveNode4, ram, start, []),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode3, [node(), SlaveNode1, SlaveNode2, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode4, [node(), SlaveNode1, SlaveNode2, SlaveNode3]),

    %% ref
    Key = "concurrent-key",
    TestPid = self(),
    Iterations = 250,
    MainNode = node(),

    %% concurrent test
    WorkerFun = fun() ->
        lists:foldl(fun(_, DisconnectedNode) ->
            %% start pid
            RandomValue = rand:uniform(99999),
            %% loop
            (catch ram:put(Key, RandomValue)),
            %% random connect / disconnect
            case node() of
                MainNode ->
                    %% do not disconnect from anyone if main, as we need to receive test results
                    ok;

                _ ->
                    case rand:uniform(4) of
                        1 ->
                            case DisconnectedNode of
                                undefined ->
                                    %% don't disconnect from main node
                                    Nodes = [SlaveNode1, SlaveNode2, SlaveNode3, SlaveNode4] -- [node()],
                                    RandomNode = lists:nth(rand:uniform(length(Nodes)), Nodes),
                                    ram_test_suite_helper:disconnect_node(RandomNode),
                                    RandomNode;

                                _ ->
                                    ram_test_suite_helper:connect_node(DisconnectedNode),
                                    undefined
                            end;

                        _ ->
                            DisconnectedNode
                    end
            end,
            %% random sleep
            RndTime = rand:uniform(30),
            timer:sleep(RndTime)
        end, undefined, lists:seq(1, Iterations)),
        TestPid ! {done, node()}
    end,

    %% spawn
    spawn(MainNode, WorkerFun),
    spawn(SlaveNode1, WorkerFun),
    spawn(SlaveNode2, WorkerFun),
    spawn(SlaveNode3, WorkerFun),
    spawn(SlaveNode4, WorkerFun),

    %% wait for workers done
    ram_test_suite_helper:assert_received_messages([
        {done, MainNode},
        {done, SlaveNode1},
        {done, SlaveNode2},
        {done, SlaveNode3},
        {done, SlaveNode4}
    ], 60000),

    %% reconnect all
    AllSlaves = [SlaveNode1, SlaveNode2, SlaveNode3, SlaveNode4],
    lists:foreach(fun(Node) ->
        lists:foreach(fun(OtherNode) ->
            rpc:call(Node, ram_test_suite_helper, connect_node, [OtherNode])
        end, AllSlaves -- [Node])
    end, AllSlaves),
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1, SlaveNode3, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode3, [node(), SlaveNode1, SlaveNode2, SlaveNode4]),
    ram_test_suite_helper:assert_subcluster(SlaveNode4, [node(), SlaveNode1, SlaveNode2, SlaveNode3]),

    %% check results are same across network
    ram_test_suite_helper:assert_wait(
        1,
        fun() ->
            ResultLocal = ram:get(Key),
            ResultOn1 = rpc:call(SlaveNode1, ram, get, [Key]),
            ResultOn2 = rpc:call(SlaveNode2, ram, get, [Key]),
            ResultOn3 = rpc:call(SlaveNode3, ram, get, [Key]),
            ResultOn4 = rpc:call(SlaveNode4, ram, get, [Key]),

            %% if unique set is of 1 element then they all contain the same result
            Ordset = ordsets:from_list([ResultLocal, ResultOn1, ResultOn2, ResultOn3, ResultOn4]),
            ordsets:size(Ordset)
        end
    ).
