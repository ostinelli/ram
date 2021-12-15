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
-module(ram).

%% API
-export([start/0, stop/0]).
-export([get/1, get/2, fetch/1]).
-export([put/2]).
-export([update/3]).
-export([delete/1]).

-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ram),
    ok.

-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(ram).

-spec get(Key :: term()) -> Value :: term().
get(Key) ->
    get(Key, undefined).

-spec get(Key :: term(), Default :: term()) -> Value :: term().
get(Key, Default) ->
    ram_kv:get(Key, Default).

-spec fetch(Key :: term()) -> {ok, Value :: term()} | error.
fetch(Key) ->
    ram_kv:fetch(Key).

-spec put(Key :: term(), Value :: term()) -> ok | {error, Reason :: term()}.
put(Key, Value) ->
    ram_kv:put(Key, Value).

-spec update(Key :: term(), Default :: term(), function()) -> ok.
update(Key, Default, Fun) ->
    ram_kv:update(Key, Default, Fun).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    ram_kv:delete(Key).
