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

%% ===================================================================
%% @doc Exposes all of the Key Value store APIs.
%%
%% <h2>Quickstart</h2>
%% TODO.
%% @end
%% ===================================================================
-module(ram).

%% API
-export([start/0, stop/0]).
-export([start_cluster/1, stop_cluster/1]).
-export([add_node/2]).
-export([get/1, get/2, fetch/1]).
-export([put/2]).
-export([update/3]).
-export([delete/1]).

%% ===================================================================
%% API
%% ===================================================================
%% @doc Starts Ram manually.
%%
%% In most cases Ram will be started as one of your application's dependencies,
%% however you may use this helper method to start it manually.
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ram),
    ok.

%% @doc Stops Ram manually.
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(ram).

%% @doc Starts the Ram cluster.
-spec start_cluster([node()]) -> ok | {error, Reason :: term()}.
start_cluster(Nodes) ->
    %% init
    ServerIds = [ram_backbone:make_server_id(Node) || Node <- Nodes],
    Machine = {module, ram_kv, #{}},
    %% start ra
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, ra, start, [])
    end, Nodes),
    %% start cluster
    case ra:start_cluster(default, ram, Machine, ServerIds) of
        {ok, StartedIds, _NotStartedIds} when length(StartedIds) =:= length(Nodes) ->
            error_logger:info_msg("RAM[~s] Cluster started on ~p", [node(), ram_backbone:get_nodes(ServerIds)]),
            ok;

        {ok, StartedIds, NotStartedIds} ->
            error_logger:warning_msg("RAM[~s] Cluster started on ~p but but not on ~p",
                [node(), ram_backbone:get_nodes(StartedIds), ram_backbone:get_nodes(NotStartedIds)]),
            ok;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Could not start cluster on ~p: ~p", [node(), ram_backbone:get_nodes(ServerIds), Reason]),
            {error, Reason}
    end.

%% @doc Stops the Ram cluster.
-spec stop_cluster([node()]) -> ok | {error, Reason :: term()}.
stop_cluster(Nodes) ->
    %% init
    ServerIds = [ram_backbone:make_server_id(Node) || Node <- Nodes],
    case ra:delete_cluster(ServerIds) of
        {ok, _LeaderId} ->
            error_logger:info_msg("RAM[~s] Cluster stopped on ~p", [node(), ram_backbone:get_nodes(ServerIds)]),
            ok;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Could not stop cluster on ~p: ~p", [node(), ram_backbone:get_nodes(ServerIds), Reason]),
            {error, Reason}
    end.

-spec add_node(Node :: node(), RefNode :: node()) -> ok | {error, Reason :: term()}.
add_node(Node, RefNode) ->
    %% init
    LeaderId = ram_backbone:make_server_id(RefNode),
    ServerId = ram_backbone:make_server_id(Node),
    %% start ra
    ok = rpc:call(Node, ra, start, []),
    %% add
    case ra:add_member(LeaderId, ServerId) of
        {ok, _, NewLeaderId} ->
            ram_backbone:maybe_update_leader_id(LeaderId, NewLeaderId),
            Machine = {module, ram_kv, #{}},
            case ra:start_server(default, ram, ServerId, Machine, [LeaderId]) of
                ok ->
                    error_logger:info_msg("RAM[~s] Node ~s added to cluster", [node(), Node]),
                    ok;

                {error, Reason} ->
                    error_logger:error_msg("RAM[~s] Error addding node ~s to cluster: ~p", [node(), Node, Reason]),
                    {error, Reason}
            end;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Error addding node ~s to cluster: ~p", [node(), Node, Reason]),
            {error, Reason}
    end.

%% @equiv get(Key, undefined)
%% @end
-spec get(Key :: term()) -> Value :: term().
get(Key) ->
    get(Key, undefined).

%% @doc Returns the Key's Value or Default if the Key is not found.
%%
%% <h2>Examples</h2>
%% <h3>Elixir</h3>
%% ```
%% iex(1)> :ram.get("key")
%% :undefined
%% iex(2)> :ram.get("key", "default")
%% "default"
%% iex(3)> :ram.put("key", "value")
%% :ok
%% iex(4)> :ram.get("key")
%% "value"
%% '''
%% <h3>Erlang</h3>
%% ```
%% 1> ram:get("key").
%% undefined
%% 2> ram:get("key", "default").
%% "default"
%% 3> ram:put("key", "value").
%% ok
%% 4> ram:get("key").
%% "value"
%% '''
-spec get(Key :: term(), Default :: term()) -> Value :: term().
get(Key, Default) ->
    ram_kv:get(Key, Default).

%% @doc Looks up a Key.
%%
%% Returns `error' if the Key is not found.
-spec fetch(Key :: term()) -> {ok, Value :: term()} | error.
fetch(Key) ->
    ram_kv:fetch(Key).

%% @doc Puts a Value for a Key.
-spec put(Key :: term(), Value :: term()) -> ok.
put(Key, Value) ->
    ram_kv:put(Key, Value).

%% @doc Atomically updates a Key with the given function.
%%
%% If Key is found then the existing Value is passed to the fun and its result is used as the updated Value of Key.
%% If Key is not found, Default is put as the Value of Key. The Default value will not be passed through the update function.
%%
%% <h2>Examples</h2>
%% <h3>Elixir</h3>
%% ```
%% iex(1)> update_fun = fn existing_value -> existing_value * 2 end
%% #Function<44.65746770/1 in :erl_eval.expr/5>
%% iex(2)> :ram.update("key", 10, update_fun)
%% ok
%% iex(3)> :ram.get("key")
%% 10
%% iex(4)> :ram.update("key", 10, update_fun)
%% ok
%% iex(5)> :ram.get("key")
%% 20
%% '''
%% <h3>Erlang</h3>
%% ```
%% 1> UpdateFun = fun(ExistingValue) -> ExistingValue * 2 end.
%% #Fun<erl_eval.44.65746770>
%% 2> ram:update("key", 10, UpdateFun).
%% ok
%% 3> ram:get("key").
%% 10
%% 4> ram:update("key", 10, UpdateFun).
%% ok
%% 5> ram:get("key").
%% 20
%% '''
-spec update(Key :: term(), Default :: term(), function()) -> ok.
update(Key, Default, Fun) ->
    ram_kv:update(Key, Default, Fun).

%% @doc Deletes a Key.
-spec delete(Key :: term()) -> ok.
delete(Key) ->
    ram_kv:delete(Key).
