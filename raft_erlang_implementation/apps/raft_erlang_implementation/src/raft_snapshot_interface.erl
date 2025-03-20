-module(raft_snapshot_interface).

-include("rpc_record.hrl").

%% API
-export([create_snapshot/3]).

-callback snapshot() ->
  ok.


-callback upsert_snapshot() -> ok.
-callback get_snapshot() -> ok.

