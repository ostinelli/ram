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
%% @private
-module(ram_kv).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([subcluster_nodes/0]).
-export([get/2, fetch/1]).
-export([put/2]).
-export([update/3]).
-export([delete/1]).

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

%% macros
-define(TRANSACTION_TIMEOUT, 5000).

%% records
-record(state, {}).

%% includes
-include("ram.hrl").

- if (?OTP_RELEASE >= 23).
-define(ETS_OPTIMIZATIONS, [{decentralized_counters, true}]).
-else.
-define(ETS_OPTIMIZATIONS, []).
-endif.

%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Options = [],
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], Options).

-spec subcluster_nodes() -> [node()] | not_running.
subcluster_nodes() ->
    try ordsets:to_list(get_subcluster_nodes())
    catch error:badarg -> not_running
    end.

-spec get(Key :: term(), Default :: term()) -> Value :: term().
get(Key, Default) ->
    case fetch(Key) of
        error -> Default;
        {ok, Value} -> Value
    end.

-spec fetch(Key :: term()) -> {ok, Value :: term()} | error.
fetch(Key) ->
    lock(Key, fun() ->
        case ets:lookup(?TABLE_STORE, Key) of
            [] -> error;
            [{Key, _Value, _Time, true}] -> error;
            [{Key, Value, _Time, _Deleted}] -> {ok, Value}
        end
    end).

-spec put(Key :: term(), Value :: term()) -> ok.
put(Key, Value) ->
    lock(Key, fun() ->
        Method = insert,
        Params = [{Key, Value, erlang:system_time(), false}],
        transaction_call(Method, Params)
    end).

-spec update(Key :: term(), Default :: term(), function()) -> ok.
update(Key, Default, Fun) ->
    lock(Key, fun() ->
        Value = case ets:lookup(?TABLE_STORE, Key) of
            [] -> Default;
            [{Key, _V, _Time, true}] -> Default;
            [{Key, V, _Time, _Deleted}] -> Fun(V)
        end,
        Method = insert,
        Params = [{Key, Value, erlang:system_time(), false}],
        transaction_call(Method, Params)
    end).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    lock(Key, fun() ->
        Method = insert,
        Params = [{Key, undefined, erlang:system_time(), true}],
        case ets:lookup(?TABLE_STORE, Key) of
            [] -> ok;
            [{Key, _V, _Time, true}] -> ok;
            _ -> transaction_call(Method, Params)
        end
    end).

-spec lock(Key :: term(), Fun :: function()) -> any().
lock(Key, Fun) ->
    global:trans({{?MODULE, Key}, self()}, Fun, subcluster_nodes()).

-spec transaction_call(Method :: atom(), Params :: [term()]) -> ok.
transaction_call(Method, Params) ->
    Tid = make_ref(),
    Nodes = [node() | subcluster_nodes()],
    case gen_server:multi_call(Nodes, ?MODULE, {'1.0', prepare_transaction, Tid, Method, Params}, ?TRANSACTION_TIMEOUT) of
        {_Replies, []} ->
            %% everyone replied -> send confirmation (wait for call response to unlock the transaction)
            _ = gen_server:multi_call(Nodes, ?MODULE, {'1.0', commit_transaction, Tid}),
            %% return
            ok;

        {_Replies, BadNodes} ->
            %% not everyone replied
            error({commit_timeout, {bad_nodes, BadNodes}})
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
    %% create tables
    ets:new(?TABLE_STORE, [set, protected, named_table, {read_concurrency, true}] ++ ?ETS_OPTIMIZATIONS),
    ets:new(?TABLE_TRANSACTIONS, [set, protected, named_table, {read_concurrency, true}] ++ ?ETS_OPTIMIZATIONS),
    ets:new(?TABLE_SHARED, [set, protected, named_table, {read_concurrency, true}] ++ ?ETS_OPTIMIZATIONS),
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
handle_call({'1.0', prepare_transaction, Tid, Method, Params}, _From, State) ->
    %% prepare transaction
    prepare_transaction(Tid, Method, Params),
    %% return
    {reply, ok, State};

handle_call({'1.0', commit_transaction, Tid}, _From, State) ->
    %% commit
    commit_transaction(Tid),
    %% return
    {reply, ok, State};

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
handle_info({'1.0', RemotePid, syn}, State) ->
    RemoteNode = node(RemotePid),
    error_logger:info_msg("RAM[~s] Received SYN from node ~s", [node(), RemoteNode]),
    %% send local entries to remote
    RemotePid ! {'1.0', self(), ack, all_local_entries()},
    %% is this a new node?
    Nodes = get_subcluster_nodes(),
    case ordsets:is_element(RemoteNode, Nodes) of
        true ->
            %% already known, ignore
            {noreply, State};

        false ->
            %% monitor
            _MRef = monitor(process, RemotePid),
            %% store new node
            _ = set_subcluster_nodes(ordsets:add_element(RemoteNode, Nodes)),
            %% return
            {noreply, State}
    end;

handle_info({'1.0', RemotePid, ack, RemoteEntries}, State) ->
    RemoteNode = node(RemotePid),
    error_logger:info_msg("RAM[~s] Received ACK from node ~s with ~w entries", [node(), RemoteNode, length(RemoteEntries)]),
    %% save remote entries to local
    merge(RemoteNode, RemoteEntries),
    %% is this a new node?
    Nodes = get_subcluster_nodes(),
    case ordsets:is_element(RemoteNode, Nodes) of
        true ->
            %% already known
            {noreply, State};

        false ->
            %% monitor
            _MRef = monitor(process, RemotePid),
            %% send local entries to remote
            RemotePid ! {'1.0', self(), ack, all_local_entries()},
            %% store new node
            _ = set_subcluster_nodes(ordsets:add_element(RemoteNode, Nodes)),
            %% return
            {noreply, State}
    end;

handle_info({'DOWN', _MRef, process, Pid, Reason}, State) ->
    RemoteNode = node(Pid),
    Nodes = get_subcluster_nodes(),
    case ordsets:is_element(RemoteNode, Nodes) of
        true ->
            error_logger:info_msg("RAM[~s] ram process is DOWN on node ~s: ~p", [node(), RemoteNode, Reason]),
            _ = set_subcluster_nodes(ordsets:del_element(RemoteNode, Nodes)),
            {noreply, State};

        _ ->
            error_logger:error_msg("RAM[~s] Received unknown DOWN message from process ~p on node ~s: ~p", [node(), Pid, Reason]),
            {noreply, State}
    end;

handle_info({nodedown, _Node}, State) ->
    %% ignore (wait for down message)
    {noreply, State};

handle_info({nodeup, RemoteNode}, State) ->
    error_logger:info_msg("RAM[~s] Node ~s has joined the cluster, sending SYN message", [node(), RemoteNode]),
    {?MODULE, RemoteNode} ! {'1.0', self(), syn},
    {noreply, State};

handle_info({transaction_timeout, Tid}, State) ->
    error_logger:info_msg("RAM[~s] Transaction ~p timed out, removing from temp", [node(), Tid]),
    remove_transaction_from_temp(Tid),
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
    case nodes() of
        [] ->
            error_logger:info_msg("RAM[~s] Running on single node", [node()]),
            {noreply, State};

        Nodes ->
            error_logger:info_msg("RAM[~s] Sending SYN to cluster", [node()]),
            %% broadcast
            lists:foreach(fun(RemoteNode) ->
                {?MODULE, RemoteNode} ! {'1.0', self(), syn}
            end, Nodes),
            {noreply, State}
    end.

%% ===================================================================
%% Internal
%% ===================================================================
-spec get_subcluster_nodes() -> Nodes :: ordsets:ordset().
get_subcluster_nodes() ->
    case ets:lookup(?TABLE_SHARED, subcluster_nodes) of
        [] -> ordsets:new();
        [{_, Nodes}] -> Nodes
    end.

-spec set_subcluster_nodes(Nodes :: ordsets:ordset()) -> true.
set_subcluster_nodes(Nodes) ->
    true = ets:insert(?TABLE_SHARED, {subcluster_nodes, Nodes}).

-spec prepare_transaction(
    Tid :: reference(),
    Method :: atom(),
    Params :: [term()]
) -> true.
prepare_transaction(Tid, Method, Params) ->
    {ok, TRef} = timer:send_after(?TRANSACTION_TIMEOUT, {transaction_timeout, Tid}),
    ets:insert(?TABLE_TRANSACTIONS, {Tid, TRef, Method, Params}).

-spec commit_transaction(Tid :: reference()) -> any().
commit_transaction(Tid) ->
    case ets:lookup(?TABLE_TRANSACTIONS, Tid) of
        [] ->
            error_logger:error_msg("RAM[~s] Received commit for untracked transaction ~p", [Tid]);

        [{Tid, TRef, Method, Params}] ->
            {ok, cancel} = timer:cancel(TRef),
            remove_transaction_from_temp(Tid),
            apply_to_ets(Method, Params)
    end.

-spec remove_transaction_from_temp(Tid :: reference()) -> true.
remove_transaction_from_temp(Tid) ->
    true = ets:delete(?TABLE_TRANSACTIONS, Tid).

-spec apply_to_ets(Method :: atom(), Params :: [term()]) -> true.
apply_to_ets(Method, Params) ->
    apply(ets, Method, [?TABLE_STORE] ++ Params).

-spec all_local_entries() -> [ram_entry()].
all_local_entries() ->
    ets:tab2list(?TABLE_STORE).

-spec merge(RemoteNode :: node(), [ram_entry()]) -> any().
merge(_RemoteNode, []) -> ok;
merge(RemoteNode, [{Key, RemoteValue, RemoteTime, RemoteDeleted} | RemoteEntries]) ->
    case ets:lookup(?TABLE_STORE, Key) of
        [] ->
            %% key not local, insert
            ets:insert(?TABLE_STORE, {Key, RemoteValue, RemoteTime, RemoteDeleted});

        [{Key, LocalValue, LocalTime, Deleted}] ->
            %% conflict
            case ram_event_handler:do_resolve_conflict(
                Key,
                {node(), LocalValue, LocalTime, Deleted},
                {RemoteNode, RemoteValue, RemoteTime, RemoteDeleted}
            ) of
                {ok, ValueToKeep, DeletedToKeep} -> ets:insert(?TABLE_STORE, {Key, ValueToKeep, erlang:system_time(), DeletedToKeep});
                error -> ets:delete(?TABLE_STORE, Key)
            end
    end,
    merge(RemoteNode, RemoteEntries).
