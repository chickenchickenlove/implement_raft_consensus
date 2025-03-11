-module(raft_snapshot_ets).

-behaviour(raft_snapshot_interface).

%% API
-export([snapshot/0]).

snapshot() ->
  ok.

