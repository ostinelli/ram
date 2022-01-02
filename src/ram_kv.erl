%% ==========================================================================================================
%% Ram - An in-memory distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2021-2022 Roberto Ostinelli <roberto@ostinelli.net>.
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
-module(ram_kv).
-behaviour(ra_machine).

%% API
-export([get/2, fetch/1]).
-export([put/2]).
-export([update/3]).
-export([delete/1]).

%% callbacks
-export([init/1, apply/3]).

-type ram_kv_command() ::
{fetch, Key :: term()} |
{put, Key :: term(), Value :: term()} |
{update, Key :: term(), Default :: term(), UpdateFun :: function()} |
{delete, Key :: term()}.

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
    ServerLoc = ram_backbone:get_server_loc(),
    case ra:consistent_query(ServerLoc,
        fun(State) ->
            maps:find(Key, State)
        end) of
        {ok, {ok, Value}, _} -> {ok, Value};
        {ok, error, _} -> error;
        {error, Reason} -> error({ram, Reason});
        {timeout, ServerId} -> error({ram, {timeout, ServerId}})
    end.

-spec put(Key :: term(), Value :: term()) -> ok.
put(Key, Value) ->
    process_command({put, Key, Value}).

-spec update(Key :: term(), Default :: term(), function()) -> ok.
update(Key, Default, Fun) ->
    process_command({update, Key, Default, Fun}).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    process_command({delete, Key}).

-spec process_command(ram_kv_command()) -> Reply :: term().
process_command(Command) ->
    ServerLoc = ram_backbone:get_server_loc(),
    case ra:process_command(ServerLoc, Command) of
        {ok, Reply, _NewServerLoc} ->
            Reply;

        {error, Reason} ->
            error({ram, Reason});

        {timeout, ServerLoc} ->
            error({ram, {timeout, ServerLoc}})
    end.

%% ===================================================================
%% Callbacks
%% ===================================================================

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init(Conf :: ra:machine_init_args()) -> map().
init(_Config) ->
    #{}.

%% ----------------------------------------------------------------------------------------------------------
%% Apply messages
%% ----------------------------------------------------------------------------------------------------------
-spec apply(ra:command_meta_data(), ram_kv_command(), map()) ->
    {map(), term(), ra_machine:effects()} | {map(), term()}.
apply(_Meta, {put, Key, Value}, State) ->
    Effects = [],
    {maps:put(Key, Value, State), ok, Effects};
apply(_Meta, {fetch, Key}, State) ->
    Effects = [],
    {State, maps:find(Key, State), Effects};
apply(_Meta, {update, Key, Default, Fun}, State) ->
    Value = case maps:find(Key, State) of
        error -> Default;
        {ok, V} -> Fun(V)
    end,
    Effects = [],
    {maps:put(Key, Value, State), ok, Effects};
apply(_Meta, {delete, Key}, State) ->
    Effects = [],
    {maps:remove(Key, State), ok, Effects}.
