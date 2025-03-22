-module(raft_snapshot_mock).

-include("rpc_record.hrl").
-behaviour(raft_snapshot_interface).

%% API
-export([]).



%% API
-export([create_snapshot/3]).
-export([upsert_snapshot/1]).

-define(DEFAULT_KEY, raft_snapshot).

create_snapshot(LocalRaftState, LastIncludeIndex, LastIncludedTerm) ->
  #raft_snapshot{
    local_raft_state=LocalRaftState,
    last_included_index=LastIncludeIndex,
    last_included_term=LastIncludedTerm
  }.

upsert_snapshot(Snapshot) ->
  self() ! Snapshot.