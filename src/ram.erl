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

%% ===================================================================
%% @doc Exposes all of the Key Value store APIs.
%% @end
%% ===================================================================
-module(ram).

%% API
-export([start/0, stop/0]).

%% ===================================================================
%% API
%% ===================================================================
%% @doc Starts Ram manually.
%%
%% In most cases Ram will be started as one of your application's dependencies,
%% however you may use this helper method to start it manually.
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ram),
    ok.

%% @doc Stops Ram manually.
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(ram).
