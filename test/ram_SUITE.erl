%% ==========================================================================================================
%% Ram - An in-memory distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2021-2022 Roberto Ostinelli <roberto@ostinelli.net>.
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
    three_nodes_operations/1
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
        {group, three_nodes}
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
        {three_nodes, [shuffle], [
            three_nodes_operations
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
init_per_group(three_nodes, Config) ->
    do_init_per_group(three_nodes, 3, Config);
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
end_per_group(three_nodes, Config) ->
    ram_test_suite_helper:end_cluster(3, Config);
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
    ok = ram:start_cluster([node()]),

    %% check
    undefined = ram:get("key"),

    %% put
    ram:put("key", "value"),
    "value" = ram:get("key"),

    %% delete
    ok = ram:delete("key"),
    undefined = ram:get("key"),

    %% stop cluster
    ok = ram:stop_cluster([node()]).

three_nodes_operations(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% start ram
    ok = ram:start(),
    ok = rpc:call(SlaveNode1, ram, start, []),
    ok = rpc:call(SlaveNode2, ram, start, []),

    %% create cluster
    ok = ram:start_cluster([node(), SlaveNode1, SlaveNode2]),

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
    20 = rpc:call(SlaveNode2, ram, get, [ComplexKey]),

    %% stop cluster
    ok = ram:stop_cluster([node(), SlaveNode1, SlaveNode2]).
