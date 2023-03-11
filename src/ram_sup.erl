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
-module(ram_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_ram_node/1]).

%% supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> supervisor:startlink_ret().
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_ram_node([node()]) -> ok | {error, supervisor:startchild_err()}.
start_ram_node(ClusterNodes) ->
  ChildSpecs = #{
    id => ram_node,
    start => {ram_node, start_link, [ClusterNodes]},
    type => worker,
    shutdown => 10000,
    restart => permanent,
    modules => [ram_node]
  },
  case supervisor:start_child(?MODULE, ChildSpecs) of
    {ok, _} -> ok;
    {ok, _, _} -> ok;
    {error, Reason} -> {error, Reason}
  end.

%% ===================================================================
%% Callbacks
%% ===================================================================
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | ignore.
init([]) ->
  Children = [],
  {ok, {{one_for_one, 10, 10}, Children}}.
