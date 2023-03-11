%% ==========================================================================================================
%% Ram - A distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2023 Roberto Ostinelli <roberto@ostinelli.net>.
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
-module(ram_node).
-behaviour(gen_statem).

%% API
-export([
  start_link/0
]).

%% states
-export([noop/3]).

%% gen_statem callbacks
-export([
  callback_mode/0,
  init/1,
  terminate/3,
  code_change/4
]).

%% records
-record(data, {}).

%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
  Options = [],
  gen_statem:start_link({local, ?MODULE}, ?MODULE, [], Options).

%% ===================================================================
%% Callbacks
%% ===================================================================
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() ->
  [state_functions, state_enter].

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init([]) ->
  {ok, State :: atom(), #data{}} |
  {ok, State :: atom(), #data{}, Actions :: [gen_statem:action()] | gen_statem:action()} |
  ignore |
  {stop, Reason :: term()}.
init([]) ->
  %% init
  {ok, noop, #data{}}.

%% ----------------------------------------------------------------------------------------------------------
%% noop state
%% ----------------------------------------------------------------------------------------------------------
-spec noop(
    enter | gen_statem:external_event_type() | gen_statem:timeout_event_type() | internal,
    EventContent :: term(),
    #data{}
) ->
  {next_state, State :: atom(), #data{}} |
  {next_state, State :: atom(), #data{}, Actions :: [gen_statem:enter_action()] | gen_statem:enter_action()} |
  gen_statem:state_callback_result(gen_statem:enter_action()).
noop(enter, _OldState, Data) ->
  {keep_state, Data};
noop(EventType, EventContent, Data) ->
  error_logger:warning_msg("RAM[~s] Received an unknown event ~p: ~p", [node(), EventType, EventContent]),
  {keep_state, Data}.

%% ----------------------------------------------------------------------------------------------------------
%% Terminate
%% ----------------------------------------------------------------------------------------------------------
-spec terminate(Reason :: term(), State :: atom(), #data{}) -> terminated.
terminate(Reason, _State, _Data) ->
  error_logger:info_msg("RAM[~s] Terminating with reason: ~p", [node(), Reason]),
  %% return
  terminated.

%% ----------------------------------------------------------------------------------------------------------
%% Convert process state when code is changed.
%% ----------------------------------------------------------------------------------------------------------
-spec code_change(OldVsn :: term(), OldState :: atom(), OldData :: #data{}, Extra :: term()) ->
  {ok, NewState :: atom(), NewData :: #data{}}.
code_change(_OldVsn, OldState, OldData, _Extra) ->
  {ok, OldState, OldData}.
