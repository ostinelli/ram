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
    handle_continue/2,
    terminate/2,
    code_change/3
]).

%% records
-record(state, {
    sync_requested = false :: boolean(),
    sync_done = false :: boolean()
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
                [] -> undefined;
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
    %% empty local database
    true = ets:delete_all_objects(?TABLE),
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
handle_call({put, Key, Value, Version}, From, State) ->
    VersionMatch = case ets:lookup(?TABLE, Key) of
        [] ->
            case Version of
                undefined -> ok;
                _ -> {error, deleted}
            end;
        [{_, _, Version}] ->
            ok;

        _ ->
            {error, outdated}
    end,
    case VersionMatch of
        ok ->
            spawn(fun() ->
                Nodes = nodes(),
                Version1 = generate_id(),
                %% send
                lists:foreach(fun(RemoteNode) ->
                    {?MODULE, RemoteNode} ! {'1.0', self(), put, Key, Value, Version1}
                end, Nodes),
                %% wait for confirmation
                receive_put_ack(Key, Value, Version1, Nodes),
                %% TODO: rollback on timeout / errors
                %% insert
                true = ets:insert(?TABLE, {Key, Value, Version1}),
                %% reply
                gen_server:reply(From, {ok, Version1})
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

        [{_, _, _}] ->
            spawn(fun() ->
                Nodes = nodes(),
                %% send
                lists:foreach(fun(RemoteNode) ->
                    {?MODULE, RemoteNode} ! {'1.0', self(), delete, Key}
                end, Nodes),
                %% wait for confirmation
                receive_delete_ack(Key, Nodes),
                %% TODO: rollback on timeout / errors
                %% delete
                true = ets:delete(?TABLE, Key),
                %% reply
                gen_server:reply(From, ok)
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
handle_info({'1.0', RemotePid, put, Key, Value, Version}, State) ->
    %% insert
    true = ets:insert(?TABLE, {Key, Value, Version}),
    %% reply
    RemotePid ! {self(), put, Key, Value, Version},
    %% return
    {noreply, State};

handle_info({'1.0', RemotePid, delete, Key}, State) ->
    %% delete
    true = ets:delete(?TABLE, Key),
    %% reply
    RemotePid ! {self(), delete, Key},
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

handle_info({'1.0', RemotePid, ack}, State) ->
    error_logger:info_msg("RAM[~s] Received ACK from node ~s", [node(), node(RemotePid)]),
    %% return
    {noreply, State};

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
    true = ets:insert(?TABLE, RemoteData),
    %% return
    {noreply, State};

handle_info(Info, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown info message: ~p", [node(), Info]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Continue messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_continue(Info :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), #state{}}.
handle_continue(after_init, State) ->
    error_logger:info_msg("RAM[~s] Sending SYN to the cluster", [node()]),
    lists:foreach(fun(RemoteNode) ->
        {?MODULE, RemoteNode} ! {'1.0', self(), syn}
    end, nodes()),
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
-spec receive_put_ack(Key :: term(), Value :: term(), Version :: term(), Nodes :: [node()]) -> ok.
receive_put_ack(_Key, _Value, _Version, []) -> ok;
receive_put_ack(Key, Value, Version, Nodes) ->
    receive
        {Pid, put, Key, Value, Version} ->
            receive_put_ack(Key, Value, Version, lists:delete(node(Pid), Nodes))
    end.

-spec receive_delete_ack(Key :: term(), Nodes :: [node()]) -> ok.
receive_delete_ack(_Key, []) -> ok;
receive_delete_ack(Key, Nodes) ->
    receive
        {Pid, delete, Key} ->
            receive_delete_ack(Key, lists:delete(node(Pid), Nodes))
    end.

-spec generate_id() -> binary().
generate_id() ->
    binary:encode_hex(crypto:hash(sha256, erlang:term_to_binary({node(), erlang:system_time()}))).
