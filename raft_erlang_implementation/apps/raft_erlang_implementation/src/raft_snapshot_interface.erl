-module(raft_snapshot_interface).

%% API
-export([]).

-callback snapshot() ->
  ok.


-callback upsert_snapshot() -> ok.
-callback get_snapshot() -> ok.

create_snapshot(LocalState,) ->

