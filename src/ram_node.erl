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
%% @private
-module(ram_node).
-behaviour(gen_statem).

%% API
-export([
  start_link/1,
  nodes/0
]).

%% states
-export([follower/3]).

%% gen_statem callbacks
-export([
  callback_mode/0,
  init/1,
  terminate/3,
  code_change/4
]).

%% records
-record(data, {
  cluster_nodes = [] :: [node()]
}).

%% types
-type state() :: follower | candidate | leader.
-export_type([state/0]).

%% ===================================================================
%% API
%% ===================================================================
-spec start_link(ClusterNodes :: [node()]) -> gen_statem:start_ret().
start_link(ClusterNodes) ->
  Options = [],
  gen_statem:start_link({local, ?MODULE}, ?MODULE, [ClusterNodes], Options).

-spec nodes() -> [{node(), state()}].
nodes() ->
  gen_statem:call(?MODULE, nodes).

%% ===================================================================
%% Callbacks
%% ===================================================================
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() ->
  [state_functions, state_enter].

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init([ClusterNodes :: [node()]]) -> gen_statem:init_result(gen_statem:state_name()).
init([ClusterNodes]) ->
  %% init
  {ok, follower, #data{
    cluster_nodes = ClusterNodes
  }}.

%% ----------------------------------------------------------------------------------------------------------
%% follower state
%% ----------------------------------------------------------------------------------------------------------
-spec follower(gen_statem:event_type(), EventContent :: term(), #data{}) ->
  {next_state, gen_statem:state_name(), #data{}} |
  {next_state, gen_statem:state_name(), #data{}, Actions :: [gen_statem:enter_action()] | gen_statem:enter_action()} |
  gen_statem:state_callback_result(gen_statem:enter_action()).
follower(enter, _OldState, _Data) ->
  keep_state_and_data;
follower({call, From}, nodes, #data{cluster_nodes = ClusterNodes}) ->
  {keep_state_and_data, {reply, From, ClusterNodes}};
follower(EventType, EventContent, _Data) ->
  error_logger:warning_msg("RAM[~s] Received an unknown event while follower ~p: ~p", [node(), EventType, EventContent]),
  keep_state_and_data.

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
