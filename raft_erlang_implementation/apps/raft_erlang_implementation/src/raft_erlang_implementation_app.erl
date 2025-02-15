%%%-------------------------------------------------------------------
%% @doc raft_erlang_implementation public API
%% @end
%%%-------------------------------------------------------------------

-module(raft_erlang_implementation_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    raft_erlang_implementation_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
