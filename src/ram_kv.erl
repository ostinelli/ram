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
-module(ram_kv).

%% API
-export([get/2, fetch/1]).
-export([put/2]).
-export([delete/1]).

%% includes
-include("ram.hrl").

%% ===================================================================
%% API
%% ===================================================================
-spec get(Key :: term(), Default :: term()) -> Value :: term().
get(Key, Default) ->
    case fetch(Key) of
        error -> Default;
        {ok, Value} -> Value
    end.

-spec fetch(Key :: term()) -> {ok, Value :: term()} | error.
fetch(Key) ->
    F = fun() ->
        case mnesia:read({ram_table, Key}) of
            [] -> error;
            [#ram_table{value = Value}] -> {ok, Value}
        end
    end,
    mnesia:activity(transaction, F).

-spec put(Key :: term(), Value :: term()) -> ok | {error, Reason :: term()}.
put(Key, Value) ->
    F = fun() ->
        mnesia:write(#ram_table{
            key = Key,
            value = Value
        })
    end,
    mnesia:activity(transaction, F).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    F = fun() -> mnesia:delete({ram_table, Key}) end,
    mnesia:activity(transaction, F).
