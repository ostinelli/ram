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
-export([get/1]).
-export([put/3]).
-export([delete/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% internals
-export([do_put/3]).
-export([do_delete/1]).

%% records
-record(state, {
    sync_requested = false :: boolean()
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

-spec get(Key :: term()) -> Value :: term().
get(Key) ->
    global:trans({{?MODULE, Key}, self()},
        fun() ->
            case ets:lookup(?TABLE, Key) of
                [] -> {error, undefined};
                [{Key, Value, Version}] -> {ok, Value, Version}
            end
        end).

-spec put(Key :: term(), Value :: term(), Version :: term()) -> ok | {error, Reason :: term()}.
put(Key, Value, Version) ->
    global:trans({{?MODULE, Key}, self()},
        fun() ->
            gen_server:call(?MODULE, {put, Key, Value, Version})
        end).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    global:trans({{?MODULE, Key}, self()},
        fun() ->
            gen_server:call(?MODULE, {delete, Key})
        end).

%% ===================================================================
%% Callbacks
%% ===================================================================

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init([]) ->
    {ok, #state{}} |
    {ok, #state{}, Timeout :: non_neg_integer()} |
    ignore |
    {stop, Reason :: term()}.
init([]) ->
    %% monitor nodes
    ok = net_kernel:monitor_nodes(true),
    %% empty local database
    true = ets:delete_all_objects(?TABLE),
    %% init
    {ok, #state{}}.

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
handle_call({put, Key, Value, Version}, From, State) ->
    {VersionMatch, TableObject} = case ets:lookup(?TABLE, Key) of
        [] ->
            case Version of
                undefined -> {ok, undefined};
                _ -> {{error, deleted}, undefined}
            end;
        [{_, _, Version} = TableObject0] ->
            {ok, TableObject0};

        _ ->
            {{error, outdated}, undefined}
    end,
    case VersionMatch of
        ok ->
            spawn_link(fun() ->
                Nodes = [node() | nodes()],
                Version1 = generate_id(),
                %% send
                Res0 = lists:foldl(fun(RemoteNode, Acc) ->
                    [rpc:call(RemoteNode, ?MODULE, do_put, [Key, Value, Version1]) | Acc]
                end, [], Nodes),
                ResSet = sets:from_list(Res0),
                Res = sets:to_list(ResSet),
                case Res of
                    [ok] ->
                        %% reply
                        gen_server:reply(From, {ok, Version1});

                    _ ->
                        %% revert
                        case TableObject of
                            {Key0, Value0, Version} ->
                                lists:foreach(fun(RemoteNode) ->
                                    rpc:call(RemoteNode, ?MODULE, do_put, [Key0, Value0, Version])
                                end, Nodes);

                            undefined ->
                                lists:foreach(fun(RemoteNode) ->
                                    rpc:call(RemoteNode, ?MODULE, do_delete, [Key])
                                end, Nodes)
                        end,
                        %% reply
                        gen_server:reply(From, {error, transaction_failed})
                end
            end),
            %% return
            {noreply, State};

        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({delete, Key}, From, State) ->
    case ets:lookup(?TABLE, Key) of
        [] ->
            {reply, {error, undefined}, State};

        [{_, Value0, Version0}] ->
            spawn_link(fun() ->
                Nodes = [node() | nodes()],
                %% send
                Res0 = lists:foldl(fun(RemoteNode, Acc) ->
                    [rpc:call(RemoteNode, ?MODULE, do_delete, [Key]) | Acc]
                end, [], Nodes),
                ResSet = sets:from_list(Res0),
                Res = sets:to_list(ResSet),
                case Res of
                    [ok] ->
                        %% reply
                        gen_server:reply(From, ok);

                    _ ->
                        %% revert
                        lists:foreach(fun(RemoteNode) ->
                            rpc:call(RemoteNode, ?MODULE, do_put, [Key, Value0, Version0])
                        end, Nodes),
                        %% reply
                        gen_server:reply(From, {error, transaction_failed})
                end
            end),
            %% return
            {noreply, State}
    end;

handle_call(Request, From, State) ->
    error_logger:warning_msg("RAM[~s] Received from ~p an unknown call message: ~p", [node(), From, Request]),
    {reply, undefined, State}.

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
handle_info({nodedown, RemoteNode}, State) ->
    error_logger:info_msg("RAM[~s] Node ~s left the cluster", [node(), RemoteNode]),
    {noreply, State};

handle_info({nodeup, RemoteNode}, State) ->
    error_logger:info_msg("RAM[~s] Node ~s joined the cluster", [node(), RemoteNode]),
    %% send syn
    {?MODULE, RemoteNode} ! {'1.0', self(), syn},
    %% return
    {noreply, State};

handle_info({'1.0', RemotePid, syn}, State) ->
    error_logger:info_msg("RAM[~s] Received SYN from node ~s", [node(), node(RemotePid)]),
    %% reply
    RemotePid ! {'1.0', self(), ack},
    %% return
    {noreply, State};

handle_info({'1.0', RemotePid, ack}, #state{sync_requested = false} = State) ->
    error_logger:info_msg("RAM[~s] Received ACK from node ~s, sending SYNC_REQ", [node(), node(RemotePid)]),
    %% request data
    RemotePid ! {'1.0', self(), sync_req},
    %% return
    {noreply, State#state{sync_requested = true}};

handle_info({'1.0', RemotePid, sync_req}, State) ->
    error_logger:info_msg("RAM[~s] Received SYNC_REQ from node ~s", [node(), node(RemotePid)]),
    %% send local data
    LocalData = ets:tab2list(?TABLE),
    RemotePid ! {'1.0', self(), sync, LocalData},
    %% return
    {noreply, State};

handle_info({'1.0', RemotePid, sync, RemoteData}, State) ->
    error_logger:info_msg("RAM[~s] Received SYNC (~w entries) from node ~s", [node(), length(RemoteData), node(RemotePid)]),
    %% store data
    merge(RemoteData),
    %% return
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

%% ===================================================================
%% Internal
%% ===================================================================
-spec do_put(Key :: term(), Value :: term(), Version :: term()) -> ok.
do_put(Key, Value, Version) ->
    true = ets:insert(?TABLE, {Key, Value, Version}),
    ok.

-spec do_delete(Key :: term()) -> ok.
do_delete(Key) ->
    true = ets:delete(?TABLE, Key),
    ok.

-spec generate_id() -> binary().
generate_id() ->
    binary:encode_hex(crypto:hash(sha256, erlang:term_to_binary({node(), erlang:system_time()}))).

-spec merge(RemoteData :: [ram_entry()]) -> any().
merge(RemoteData) ->
    %% TODO: loop for conflicts
    true = ets:insert(?TABLE, RemoteData).
