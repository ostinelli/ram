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
-export([add_node/1, remove_node/1, nodes/0]).
-export([lookup_leader/0]).
-export([process_command/1, process_query/1]).

%% tests
-ifdef(TEST).
-export([get_node_from_server_id/1]).
-endif.

%% macros
-define(SYSTEM, default).
-define(CLUSTER_NAME, ram).
-define(RA_MACHINE, {module, ram_kv, #{}}).

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

-spec add_node(Node :: node()) -> ok | {error, Reason :: term()}.
add_node(Node) ->
    %% start ra
    ok = rpc:call(Node, ra, start, []),
    %% get members
    case catch members() of
        {'EXIT', {{failed_to_get_members, _} = Reason, _}} ->
            {error, Reason};

        [] ->
            {error, failed_to_get_members};

        ServerIds ->
            ServerId = make_server_id(Node),
            case lists:member(ServerId, ServerIds) of
                false -> start_server_and_add(ServerId, ServerIds);
                true -> start_server(ServerId, ServerIds)
            end
    end.

-spec remove_node(Node :: node()) -> ok | {error, Reason :: term()}.
remove_node(Node) ->
    %% get members
    case catch members() of
        {'EXIT', {{failed_to_get_members, _} = Reason, _}} ->
            {error, Reason};

        [] ->
            {error, failed_to_get_members};

        ServerIds ->
            ServerId = make_server_id(Node),
            remove_and_stop_server(ServerId, ServerIds)
    end.

-spec nodes() -> [node()].
nodes() ->
    get_nodes_from_server_ids(members()).

-spec lookup_leader() -> ra:server_id() | undefined.
lookup_leader() ->
    ra_leaderboard:lookup_leader(?CLUSTER_NAME).

-spec process_command(ram_kv:ram_kv_command()) -> Ret :: term() | {error, Reason :: term()}.
process_command(Command) ->
    case lookup_leader() of
        undefined ->
            error_logger:error_msg("RAM[~s] Failed to lookup leader", [node()]),
            {error, could_not_lookup_leader};

        LeaderId ->
            case ra:process_command(LeaderId, Command) of
                {ok, Ret, _LeaderId} -> Ret;
                {timeout, _} = Timeout -> {error, Timeout};
                {error, _} = Error -> Error
            end
    end.

-spec process_query(QueryFun :: function()) -> Ret :: term() | {error, Reason :: term()}.
process_query(QueryFun) ->
    case lookup_leader() of
        undefined ->
            error_logger:error_msg("RAM[~s] Failed to lookup leader", [node()]),
            {error, could_not_lookup_leader};

        LeaderId ->
            case ra:consistent_query(LeaderId, QueryFun) of
                {ok, Ret, _} -> Ret;
                {timeout, _} = Timeout -> {error, Timeout};
                {error, _} = Error -> Error
            end
    end.

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

-spec members() -> [ra:server_id()].
members() ->
    ServerId = make_server_id(node()),
    case ra:members(ServerId) of
        {ok, ServerIds, _} ->
            ServerIds;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Failed to retrieve members: ~p", [node(), Reason]),
            error({failed_to_get_members, Reason})
    end.

-spec start_server_and_add(ra:server_id(), [ra:server_id()]) -> ok | {error, Reason :: term()}.
start_server_and_add(ServerId, ServerIds) ->
    case start_server(ServerId, ServerIds) of
        ok ->
            case ra:add_member(ServerIds, ServerId) of
                {ok, _, _} ->
                    error_logger:info_msg("RAM[~s] Node ~s added to cluster", [node(), get_node_from_server_id(ServerId)]),
                    ok;

                {error, Reason} ->
                    error_logger:error_msg("RAM[~s] Error adding node ~s to cluster: ~p", [node(), get_node_from_server_id(ServerId), Reason]),
                    {error, Reason}
            end;

        {error, Reason} ->
            {error, Reason}
    end.

-spec start_server(ra:server_id(), [ra:server_id()]) -> ok | {error, Reason :: term()}.
start_server(ServerId, ServerIds) ->
    case ra:start_server(?SYSTEM, ?CLUSTER_NAME, ServerId, ?RA_MACHINE, ServerIds) of
        ok ->
            error_logger:info_msg("RAM[~s] Started server", [node(), get_node_from_server_id(ServerId)]),
            ok;

        {error, Reason} ->
            error_logger:error_msg("RAM[~s] Error starting server ~s: ~p", [node(), get_node_from_server_id(ServerId), Reason]),
            {error, Reason}
    end.

-spec remove_and_stop_server(ra:server_id(), [ra:server_id()]) -> ok | {error, Reason :: term()}.
remove_and_stop_server(ServerId, ServerIds) ->
    case ra:leave_and_delete_server(?SYSTEM, ServerIds, ServerId) of
        ok ->
            error_logger:info_msg("RAM[~s] Node ~s removed from cluster", [node(), get_node_from_server_id(ServerId)]),
            ok;

        timeout ->
            error_logger:error_msg("RAM[~s] Timeout removing node ~s from cluster", [node(), get_node_from_server_id(ServerId)]),
            {error, timeout};

        {error, _} = Err ->
            error_logger:error_msg("RAM[~s] Error removing node ~s from cluster: ~p", [node(), get_node_from_server_id(ServerId), Err]),
            Err
    end.
