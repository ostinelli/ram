%% ==========================================================================================================
%% Ram - An in-memory distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2019-2021 Roberto Ostinelli <roberto@ostinelli.net> and Neato Robotics, Inc.
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
-module(ram_event_handler).

%% API
-export([ensure_event_handler_loaded/0]).
-export([do_resolve_conflict/3]).

-callback resolve_conflict(
    Key :: term(),
    {Node1 :: node(), Value1 :: term(), Time1 :: non_neg_integer()},
    {Node2 :: node(), Value2 :: term(), Time2 :: non_neg_integer()}
) -> ValueToKeep :: term().

-optional_callbacks([resolve_conflict/3]).

%% ===================================================================
%% API
%% ===================================================================
%% @private
-spec ensure_event_handler_loaded() -> ok.
ensure_event_handler_loaded() ->
    %% get handler
    CustomEventHandler = get_custom_event_handler(),
    %% ensure that is it loaded (not using code:ensure_loaded/1 to support embedded mode)
    catch CustomEventHandler:module_info(exports),
    ok.


%% @private
-spec do_resolve_conflict(
    Key :: term(),
    {Node1 :: node(), Value1 :: term(), Time1 :: non_neg_integer()},
    {Node2 :: node(), Value2 :: term(), Time2 :: non_neg_integer()}
) -> {ok, ValueToKeep :: term()} | error.
do_resolve_conflict(Key, {Node1, Value1, Time1}, {Node2, Value2, Time2}) ->
    CustomEventHandler = get_custom_event_handler(),
    case erlang:function_exported(CustomEventHandler, resolve_conflict, 3) of
        true ->
            try
                Value = CustomEventHandler:resolve_conflict(Key, {Node1, Value1, Time1}, {Node2, Value2, Time2}),
                {ok, Value}
            catch Class:Reason ->
                error_logger:error_msg(
                    "RAM[~s] Error ~p in custom handler resolve_conflict: ~p",
                    [node(), Class, Reason]
                ),
                error
            end;

        _ ->
            %% by default, keep value registered more recently
            %% NB: this is a simple mechanism that can be imprecise
            ValueToKeep = case Time1 > Time2 of
                true -> Value1;
                _ -> Value2
            end,
            {ok, ValueToKeep}
    end.

%% ===================================================================
%% Internal
%% ===================================================================
-spec get_custom_event_handler() -> undefined | {ok, CustomEventHandler :: atom()}.
get_custom_event_handler() ->
    application:get_env(ram, event_handler, undefined).
