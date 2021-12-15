%% ==========================================================================================================
%% Ram - An ephemeral distributed KV store for Erlang and Elixir.
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
-module(ram_backbone).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
    {ok, State :: map()} |
    {ok, State :: map(), Timeout :: non_neg_integer()} |
    ignore |
    {stop, Reason :: term()}.
init([]) ->
    %% init
    {ok, #{}}.

%% ----------------------------------------------------------------------------------------------------------
%% Call messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: term(), State :: map()) ->
    {reply, Reply :: term(), State :: map()} |
    {reply, Reply :: term(), State :: map(), Timeout :: non_neg_integer()} |
    {noreply, State :: map()} |
    {noreply, State :: map(), Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), Reply :: term(), State :: map()} |
    {stop, Reason :: term(), State :: map()}.
handle_call(Request, From, State) ->
    error_logger:warning_msg("RAM[~s] Received from ~p an unknown call message: ~p", [node(), From, Request]),
    {reply, undefined, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_cast(Msg :: term(), State :: map()) ->
    {noreply, State :: map()} |
    {noreply, State :: map(), Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), State :: map()}.
handle_cast(Msg, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown cast message: ~p", [node(), Msg]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% All non Call / Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_info(Info :: term(), State :: map()) ->
    {noreply, State :: map()} |
    {noreply, State :: map(), Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), State :: map()}.
handle_info(Info, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown info message: ~p", [node(), Info]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Terminate
%% ----------------------------------------------------------------------------------------------------------
-spec terminate(Reason :: term(), State :: map()) -> terminated.
terminate(Reason, _State) ->
    error_logger:info_msg("RAM[~s] Terminating with reason: ~p", [node(), Reason]),
    %% return
    terminated.

%% ----------------------------------------------------------------------------------------------------------
%% Convert process state when code is changed.
%% ----------------------------------------------------------------------------------------------------------
-spec code_change(OldVsn :: term(), State :: map(), Extra :: term()) -> {ok, State :: map()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal
%% ===================================================================
