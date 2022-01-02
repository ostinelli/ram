%% ==========================================================================================================
%% Ram - An ephemeral distributed KV store for Erlang and Elixir.
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
%% @private
-module(ram_backbone).

%% API
-export([start_cluster/1, stop_cluster/1]).
-export([add_node/2, remove_node/2, nodes/0]).
-export([get_server_loc/0]).

%% tests
-ifdef(TEST).
-export([get_node_from_server_id/1]).
-endif.

%% macros
-define(SYSTEM, default).
-define(CLUSTER_NAME, ram).
-define(RA_MACHINE,  {module, ram_kv, #{}}).

%% ===================================================================
%% API
%% ===================================================================
-spec start_cluster([node()]) -> ok | {error, Reason :: term()}.
start_cluster(Nodes) ->
    %% init
    ServerIds = [make_server_id(Node) || Node <- Nodes],
    %% start ra
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, ra, start, [])
    end, Nodes),
    %% start cluster
    case ra:start_cluster(?SYSTEM, ?CLUSTER_NAME, ?RA_MACHINE, ServerIds) of
        {ok, StartedIds, _NotStartedIds} when length(StartedIds) =:= length(Nodes) ->
            error_logger:info_msg("RAM[~s] Cluster started on ~p", [node(), get_nodes_from_server_ids(ServerIds)]),
            ok;

        {ok, StartedIds, NotStartedIds} ->
            error_logger:warning_msg("RAM[~s] Cluster started on ~p but but not on ~p",
                [node(), get_nodes_from_server_ids(StartedIds), get_nodes_from_server_ids(NotStartedIds)]),
            ok;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Could not start cluster on ~p: ~p", [node(), get_nodes_from_server_ids(ServerIds), Reason]),
            {error, Reason}
    end.

%% @doc Stops the Ram cluster.
-spec stop_cluster([node()]) -> ok | {error, Reason :: term()}.
stop_cluster(Nodes) ->
    %% init
    ServerIds = [make_server_id(Node) || Node <- Nodes],
    case ra:delete_cluster(ServerIds) of
        {ok, _ServerLoc} ->
            error_logger:info_msg("RAM[~s] Cluster stopped on ~p", [node(), get_nodes_from_server_ids(ServerIds)]),
            ok;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Could not stop cluster on ~p: ~p", [node(), get_nodes_from_server_ids(ServerIds), Reason]),
            {error, Reason}
    end.

-spec add_node(Node :: node(), RefNode :: node()) -> ok | {error, Reason :: term()}.
add_node(Node, RefNode) ->
    %% init
    ServerLoc = make_server_id(RefNode),
    ServerId = make_server_id(Node),
    %% start ra
    ok = rpc:call(Node, ra, start, []),
    %% add
    case ra:add_member(ServerLoc, ServerId) of
        {ok, _, _NewServerLoc} ->
            case ra:start_server(?SYSTEM, ?CLUSTER_NAME, ServerId, ?RA_MACHINE, [ServerLoc]) of
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

-spec remove_node(Node :: node(), RefNode :: node()) -> ok | {error, Reason :: term()}.
remove_node(Node, RefNode) ->
    %% init
    ServerLoc = make_server_id(RefNode),
    ServerId = make_server_id(Node),
    %% remove
    case ra:leave_and_delete_server(?SYSTEM, ServerLoc, ServerId) of
        ok ->
            error_logger:info_msg("RAM[~s] Node ~s removed from cluster", [node(), Node]),
            ok;

        timeout ->
            error_logger:error_msg("RAM[~s] Timeout removing node ~s from cluster", [node(), Node]),
            {error, timeout};

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Error removing node ~s from cluster: ~p", [node(), Node, Reason]),
            {error, Reason};

        {badrpc, Reason} ->
            error_logger:error_msg("RAM[~s] badrpc error removing node ~s from cluster: ~p", [node(), Node, Reason]),
            {error, Reason}
    end.

-spec nodes() -> [node()].
nodes() ->
    ServerId = make_server_id(node()),
    case ra:members(ServerId) of
        {ok, ServerIds, _} ->
            get_nodes_from_server_ids(ServerIds);

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Failed to retrieve members: ~p", [node(), Reason]),
            {error, Reason}
    end.

-spec get_server_loc() -> ra:server_id().
get_server_loc() ->
    make_server_id(node()).

%% ===================================================================
%% Internals
%% ===================================================================
-spec make_server_id(node()) -> ra:server_id().
make_server_id(Node) ->
    {ram, Node}.

-spec get_node_from_server_id(ra:server_id()) -> node().
get_node_from_server_id({ram, Node}) ->
    Node.

-spec get_nodes_from_server_ids([ra:server_id()]) -> [node()].
get_nodes_from_server_ids(ServerIds) ->
    [get_node_from_server_id(ServerId) || ServerId <- ServerIds].
