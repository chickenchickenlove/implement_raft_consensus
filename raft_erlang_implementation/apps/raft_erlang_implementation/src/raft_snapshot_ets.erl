-module(raft_snapshot_ets).

-include("rpc_record.hrl").

-behaviour(raft_snapshot_interface).

%% API
-export([create_snapshot/3]).
-export([upsert_snapshot/1]).

-define(DEFAULT_KEY, raft_snapshot).

create_snapshot(RaftState, LastIncludeIndex, LastIncludedTerm) ->
  #raft_snapshot{
    raft_state=RaftState,
    last_included_index=LastIncludeIndex,
    last_included_term=LastIncludedTerm
  }.

upsert_snapshot(Snapshot) ->
  MyName = raft_util:my_name(),
  new_ets_if_not_existed(MyName),
  ets:insert(MyName, {?DEFAULT_KEY, Snapshot}).

%% @private
%% TODO : Spec
new_ets_if_not_existed(MyName) ->
  try
    ets:new(MyName, [named_table, set, public])
  catch
    _:_ -> ok
  end.
