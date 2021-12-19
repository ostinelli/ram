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
-module(ram_kv).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([subcluster_nodes/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

%% records
-record(state, {
    nodes_map = #{} :: #{node() => pid()}
}).

%% includes
-include("ram.hrl").

%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Options = [],
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], Options).

-spec subcluster_nodes() -> [node()] | not_running.
subcluster_nodes() ->
    try gen_server:call(?MODULE, subcluster_nodes)
    catch exit:{noproc, {gen_server, call, _}} -> not_running
    end.

%% ===================================================================
%% Callbacks
%% ===================================================================

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init([]) ->
    {ok, #state{}} |
    {ok, #state{}, timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    %% monitor nodes
    ok = net_kernel:monitor_nodes(true),
    %% init
    {ok, #state{}, {continue, after_init}}.

%% ----------------------------------------------------------------------------------------------------------
%% Call messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: term(), #state{}) ->
    {reply, Reply :: term(), #state{}} |
    {reply, Reply :: term(), #state{}, Timeout :: non_neg_integer()} |
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), Reply :: term(), #state{}} |
    {stop, Reason :: term(), #state{}}.
handle_call(subcluster_nodes, _From, #state{
    nodes_map = NodesMap
} = State) ->
    Nodes = maps:keys(NodesMap),
    {reply, Nodes, State};

handle_call(Request, From, State) ->
    error_logger:warning_msg("RAM[~s] Received from ~p an unknown call message: ~p", [node(), From, Request]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_cast(Msg :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), #state{}}.
handle_cast(Msg, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown cast message: ~p", [node(), Msg]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% All non Call / Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_info(Info :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), #state{}}.
handle_info({'1.0', syn, RemotePid}, #state{
    nodes_map = NodesMap
} = State) ->
    RemoteNode = node(RemotePid),
    error_logger:info_msg("RAM[~s] Received SYN from node ~s", [node(), RemoteNode]),
    %% reply
    RemotePid ! {'1.0', ack, self()},
    %% is this a new node?
    case maps:is_key(RemoteNode, NodesMap) of
        true ->
            %% already known, ignore
            {noreply, State};

        false ->
            %% monitor
            _MRef = monitor(process, RemotePid),
            {noreply, State#state{nodes_map = NodesMap#{RemoteNode => RemotePid}}}
    end;

handle_info({'1.0', ack, RemotePid}, #state{
    nodes_map = NodesMap
} = State) ->
    RemoteNode = node(RemotePid),
    error_logger:info_msg("RAM[~s] Received ACK from node ~s", [node(), RemoteNode]),
    %% is this a new node?
    case maps:is_key(RemoteNode, NodesMap) of
        true ->
            %% already known
            {noreply, State};

        false ->
            %% monitor
            _MRef = monitor(process, RemotePid),
            %% return
            {noreply, State#state{nodes_map = NodesMap#{RemoteNode => RemotePid}}}
    end;

handle_info({'DOWN', _MRef, process, Pid, Reason}, #state{
    nodes_map = NodesMap
} = State) ->
    RemoteNode = node(Pid),
    case maps:take(RemoteNode, NodesMap) of
        {Pid, NodesMap1} ->
            error_logger:info_msg("RAM[~s] ram process is DOWN on node ~s: ~p", [node(), RemoteNode, Reason]),
            {noreply, State#state{nodes_map = NodesMap1}};

        error ->
            %% relay to handler
            error_logger:error_msg("RAM[~s] Received unknown DOWN message from process ~p on node ~s: ~p", [node(), Pid, Reason]),
            {noreply, State}
    end;

handle_info({nodedown, _Node}, State) ->
    %% ignore (wait for down message)
    {noreply, State};

handle_info({nodeup, RemoteNode}, State) ->
    error_logger:info_msg("RAM[~s] Node ~s has joined the cluster, sending SYN message", [node(), RemoteNode]),
    {?MODULE, RemoteNode} ! {'1.0', syn, self()},
    {noreply, State};

handle_info(Info, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown info message: ~p", [node(), Info]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Terminate
%% ----------------------------------------------------------------------------------------------------------
-spec terminate(Reason :: term(), #state{}) -> terminated.
terminate(Reason, _State) ->
    error_logger:info_msg("RAM[~s] Terminating with reason: ~p", [node(), Reason]),
    %% return
    terminated.

%% ----------------------------------------------------------------------------------------------------------
%% Convert process state when code is changed.
%% ----------------------------------------------------------------------------------------------------------
-spec code_change(OldVsn :: term(), #state{}, Extra :: term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Continue messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_continue(Info :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), #state{}}.
handle_continue(after_init, State) ->
    error_logger:info_msg("RAM[~s] Sending SYN to cluster", [node()]),
    %% broadcast
    lists:foreach(fun(RemoteNode) ->
        {?MODULE, RemoteNode} ! {'1.0', syn, self()}
    end, nodes()),
    {noreply, State}.

%% ===================================================================
%% Internal
%% ===================================================================
