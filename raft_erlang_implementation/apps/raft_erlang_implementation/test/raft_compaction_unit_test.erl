-module(raft_compaction_unit_test).

-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SNAPSHOT_MOCK_MODULE, raft_snapshot_mock).

%% API
-export([]).

make_snapshot_if_needed1_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  LocalRaftState = #{a => 1, b=> 2},
  RaftLogCompactionMetadata = #raft_log_compaction_metadata{current_start_index=0},
  CommitIndex = 3,
  Entries = [{5, "A5"}, {5, "A4"}, {5, "A3"}, {3, "A2"}, {1, "A1"}],
  LastAppliedEntriesWithIndex = #raft_entry_with_index{raft_entry={5, "A3"}, raft_index=3},
  RaftConfiguration = #raft_configuration{max_log_size=10, snapshot_module=?SNAPSHOT_MOCK_MODULE},

  %%% WHEN
  Result = raft_log_compaction:make_snapshot_if_needed(LocalRaftState, RaftLogCompactionMetadata, CommitIndex, Entries, LastAppliedEntriesWithIndex, RaftConfiguration),

  %%% THEN
  {NewRaftLogCompactionMetadata, NewEntries} = Result,
  ?assertEqual(NewRaftLogCompactionMetadata, #raft_log_compaction_metadata{current_start_index=0}),
  ?assertEqual(NewEntries, Entries).


make_snapshot_if_needed2_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  LocalRaftState = #{},
  RaftLogCompactionMetadata = #raft_log_compaction_metadata{current_start_index=0},
  CommitIndex = 0,
  Entries = [],
  LastAppliedEntriesWithIndex = #raft_entry_with_index{raft_entry=undefined, raft_index=0},
  RaftConfiguration = #raft_configuration{max_log_size=10, snapshot_module=?SNAPSHOT_MOCK_MODULE},

  %%% WHEN
  Result = raft_log_compaction:make_snapshot_if_needed(LocalRaftState, RaftLogCompactionMetadata, CommitIndex, Entries, LastAppliedEntriesWithIndex, RaftConfiguration),


  %%% THEN
  {NewRaftLogCompactionMetadata, NewEntries} = Result,
  ?assertEqual(NewRaftLogCompactionMetadata, #raft_log_compaction_metadata{current_start_index=0}),
  ?assertEqual(NewEntries, Entries).


make_snapshot_if_needed3_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  LocalRaftState = #{a => 1, b=> 2},
  RaftLogCompactionMetadata = #raft_log_compaction_metadata{current_start_index=0},
  CommitIndex = 3,
  Entries = [{5, "A5"}, {5, "A4"}, {5, "A3"}, {3, "A2"}, {1, "A1"}],
  LastAppliedEntriesWithIndex = #raft_entry_with_index{raft_entry={5, "A3"}, raft_index=3},
  RaftConfiguration = #raft_configuration{max_log_size=2, snapshot_module=?SNAPSHOT_MOCK_MODULE},

  %%% WHEN
  Result = raft_log_compaction:make_snapshot_if_needed(LocalRaftState, RaftLogCompactionMetadata, CommitIndex,
                                                       Entries, LastAppliedEntriesWithIndex, RaftConfiguration),


  %%% THEN
  {NewRaftLogCompactionMetadata, NewEntries} = Result,
  ?assertEqual([{5, "A5"}, {5, "A4"}], NewEntries),
  ?assertEqual(#raft_log_compaction_metadata{current_start_index=4}, NewRaftLogCompactionMetadata),

  ExpectedSnapshot = #raft_snapshot{local_raft_state=LocalRaftState, last_included_index=3, last_included_term=5},
  ?assertEqual(ExpectedSnapshot, raft_test_util:get_msg()).


make_snapshot_if_needed4_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  LocalRaftState = #{a => 1, b=> 2},
  RaftLogCompactionMetadata = #raft_log_compaction_metadata{current_start_index=5},
  CommitIndex = 7,
  Entries = [{5, "A9"}, {5, "A8"}, {5, "A7"}, {3, "A6"}, {1, "A5"}],
  LastAppliedEntriesWithIndex = #raft_entry_with_index{raft_entry={5, "A7"}, raft_index=7},
  RaftConfiguration = #raft_configuration{max_log_size=2, snapshot_module=?SNAPSHOT_MOCK_MODULE},

  %%% WHEN
  Result = raft_log_compaction:make_snapshot_if_needed(LocalRaftState, RaftLogCompactionMetadata, CommitIndex,
    Entries, LastAppliedEntriesWithIndex, RaftConfiguration),

  %%% THEN
  {NewRaftLogCompactionMetadata, NewEntries} = Result,
  ?assertEqual([{5, "A9"}, {5, "A8"}], NewEntries),
  ?assertEqual(#raft_log_compaction_metadata{current_start_index=8}, NewRaftLogCompactionMetadata),

  ExpectedSnapshot = #raft_snapshot{local_raft_state=LocalRaftState, last_included_index=7, last_included_term=5},
  ?assertEqual(ExpectedSnapshot, raft_test_util:get_msg()).
