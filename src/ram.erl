%% ==========================================================================================================
%% Ram - A distributed KV store for Erlang and Elixir.
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
%% @end
%% ===================================================================
-module(ram).

%% API
-export([start/0, stop/0]).
-export([start_cluster/1, stop_cluster/1]).
-export([add_node/1, remove_node/1, nodes/0]).
-export([restart_server/0]).
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
    ram_backbone:start_cluster(Nodes).

%% @doc Stops the Ram cluster.
-spec stop_cluster([node()]) -> ok | {error, Reason :: term()}.
stop_cluster(Nodes) ->
    ram_backbone:stop_cluster(Nodes).

%% @doc Restart a previously stopped node of the cluster, so that it joins the cluster again.
-spec restart_server() -> ok | {error, Reason :: term()}.
restart_server() ->
    ram_backbone:restart_server().

%% @doc Adds Node to an existing Ram cluster.
%%
%% This method is to be called when:
%% <ul>
%% <li>Adding a new node to the cluster.</li>
%% <li>Restarting a previously stopped node of the cluster, so that it joins the cluster again.</li>
%% </ul>
%%
%% Note that when restarting a server it might be preferable to use {@link restart_server/0} instead.
-spec add_node(Node :: node()) -> ok | {error, Reason :: term()}.
add_node(Node) ->
    ram_backbone:add_node(Node).

%% @doc Removes Node from the Ram cluster.
-spec remove_node(Node :: node()) -> ok | {error, Reason :: term()}.
remove_node(Node) ->
    ram_backbone:remove_node(Node).

%% @doc Returns the nodes in the Ram cluster.
-spec nodes() -> [node()].
nodes() ->
    ram_backbone:nodes().

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
%% <div style="font-size:30px;float:left;padding-right:10px;color:#ffcc00;">⚠</div>
%% <i>Passing functions as arguments might not be compatible across different Erlang versions,
%% so if your cluster is composed of nodes running different Erlang versions do not use this method.</i>
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
