%% ==========================================================================================================
%% Ram - A distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2023 Roberto Ostinelli <roberto@ostinelli.net> and Neato Robotics, Inc.
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
-module(ram_test_suite_helper).

%% API
-export([init_cluster/1, end_cluster/1]).

%% includes
-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% API
%% ===================================================================
init_cluster(NodesCount) ->
  {Peers, Nodes} = lists:foldl(fun(_, {PeerAcc, NodeAcc}) ->
    %% start peer
    {ok, Peer, Node} = peer:start(#{
      connection => standard_io,
      name => peer:random_name(),
      wait_boot => 5000,
      args => [
        "-connect_all", "false", "-kernel", "dist_auto_connect", "never",
        "-pz", filename:dirname(code:which(ram)), "-pz", filename:dirname(code:which(?MODULE))
      ]
    }),
    %% connect manually
    true = peer:call(Peer, net_kernel, connect_node, [node()], 5000),
    %% start ram
    ok = peer:call(Peer, ram, start, [], 5000),
    %% return peer id
    {[Peer | PeerAcc], [Node | NodeAcc]}
  end, {[], []}, lists:seq(1, NodesCount)),
  [{peers, Peers}, {nodes, Nodes}].

end_cluster(Peers) ->
  [peer:stop(Peer) || Peer <- Peers],
  ok.
