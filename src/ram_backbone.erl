%% ==========================================================================================================
%% Ram - An ephemeral distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2015 Roberto Ostinelli <roberto@ostinelli.net> and Neato Robotics, Inc.
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
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% records
-record(state, {

}).

%% includes
-include("ram.hrl").

-if (?OTP_RELEASE >= 23).
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
    %% init db with current node set
    init_mnesia_tables(),
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
init_mnesia_tables() ->
    ClusterNodes = [node() | nodes()],
    {ok, _} = mnesia:change_config(extra_db_nodes, ClusterNodes),
    %% create tables
    create_table(ram_table, [
        {type, set},
        {ram_copies, ClusterNodes},
        {attributes, record_info(fields, ram_table)},
        {storage_properties, [{ets, [{read_concurrency, true}, {write_concurrency, true}] ++ ?ETS_OPTIMIZATIONS}]}
    ]).

-spec create_table(TableName :: atom(), Options :: [tuple()]) -> ok | {error, any()}.
create_table(TableName, Options) ->
    CurrentNode = node(),
    %% ensure table exists
    case mnesia:create_table(TableName, Options) of
        {atomic, ok} ->
            error_logger:info_msg("~p was successfully created~n", [TableName]),
            ok;
        {aborted, {already_exists, TableName}} ->
            %% table already exists, try to add current node as copy
            add_table_copy_to_current_node(TableName);
        {aborted, {already_exists, TableName, CurrentNode}} ->
            %% table already exists, try to add current node as copy
            add_table_copy_to_current_node(TableName);
        Other ->
            error_logger:error_msg("Error while creating ~p: ~p~n", [TableName, Other]),
            {error, Other}
    end.

-spec add_table_copy_to_current_node(TableName :: atom()) -> ok | {error, any()}.
add_table_copy_to_current_node(TableName) ->
    CurrentNode = node(),
    %% wait for table
    mnesia:wait_for_tables([TableName], 10000),
    %% add copy
    case mnesia:add_table_copy(TableName, CurrentNode, ram_copies) of
        {atomic, ok} ->
            error_logger:info_msg("Copy of ~p was successfully added to current node~n", [TableName]),
            ok;
        {aborted, {already_exists, TableName}} ->
            error_logger:info_msg("Copy of ~p is already added to current node~n", [TableName]),
            ok;
        {aborted, {already_exists, TableName, CurrentNode}} ->
            error_logger:info_msg("Copy of ~p is already added to current node~n", [TableName]),
            ok;
        {aborted, Reason} ->
            error_logger:error_msg("Error while creating copy of ~p: ~p~n", [TableName, Reason]),
            {error, Reason}
    end.
