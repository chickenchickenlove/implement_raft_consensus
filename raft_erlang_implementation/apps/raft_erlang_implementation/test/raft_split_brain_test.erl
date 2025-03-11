-module(raft_split_brain_test).

-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).


split_brain1_test() ->
  %%% GIVEN1 -> Raft nodes are given
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D', 'E', 'F'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers, raft_test_factory:default_raft_config()),
  raft_util:set_timer_time(150),
  timer:sleep(30),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidE} = raft_node_state_machine:start('E', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidF} = raft_node_state_machine:start('F', RaftMembers, raft_test_factory:default_raft_config()),

  %%% WHEN1 : make Node 'A' leader.
  IgnoreMembers = ['B', 'C', 'D', 'E', 'F'],
  set_ignore_msg_from_this_peer([PidB, PidC, PidD, PidE, PidF], IgnoreMembers),

  timer:sleep(500),
  unset_ignore_peer([PidB, PidC, PidD, PidE, PidF]),

  %%% THEN1 : Node A is leader
  raft_test_util:assert_node_state_equal(leader, PidA),

  %%% WHEN1 - Trigger Split Brain, and send different messages to each split clusters.
  raft_util:set_timer_time(150),
  SplitMembers1 = ['C', 'D', 'E', 'F'],
  SplitMembers2 = ['A', 'B'],

  timer:sleep(1500),

  ClusterA = [PidA, PidB],
  ClusterC = [PidC, PidD, PidE, PidF],
  set_ignore_msg_from_this_peer(ClusterA, SplitMembers1),
  set_ignore_msg_from_this_peer(ClusterC, SplitMembers2),

  raft_api:add_entry_async(PidA, "A1", self()),
  raft_api:add_entry_async(PidA, "A2", self()),

  raft_util:set_timer_time(50),
  timer:sleep(1500),

  raft_api:add_entry_async(PidC, "C1", self()),
  raft_api:add_entry_async(PidC, "C2", self()),

  timer:sleep(50),

  %%% THEN - each cluster should have different logs in its entries.
  %%% However, A Cluster cannot commit, because it don't have majority.
  SplitA_B = [PidA, PidB],
  EntriesSplitA_B0 = ["A2", "A1"],
  raft_test_util:assert_node_state_equal_exactly(1, leader, SplitA_B),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_B0, PidA),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_B0, PidB),
  raft_test_util:assert_leader_commit_index(0, PidA),

  %%% C Cluster can commit, because it has majority.
  SplitC_F = [PidC, PidD, PidE, PidF],
  EntriesSplitC_F0 = ["C2", "C1"],
  raft_test_util:assert_node_state_equal_exactly(1, leader, SplitC_F),
  raft_test_util:assert_node_state_equal_exactly(3, follower, SplitC_F),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F0, PidC),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F0, PidD),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F0, PidE),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F0, PidF),
  raft_test_util:assert_leader_commit_index(2, PidC),

  %%% WHEN : Give more entries to A, C cluster.
  raft_api:add_entry_async(PidA, "A3", self()),
  raft_api:add_entry_async(PidA, "A4", self()),

  raft_api:add_entry_async(PidC, "C3", self()),
  raft_api:add_entry_async(PidC, "C4", self()),
  raft_api:add_entry_async(PidC, "C5", self()),
  raft_api:add_entry_async(PidC, "C6", self()),
  timer:sleep(500),

  %%% THEN: A cluster has 4 entries, and C cluster has 6 more entries newly added.
  %%% But, only C Cluster can commit. so, A cluster commit index 0, C cluster commit index 6.
  EntriesSplitA_B1 = ["A4", "A3", "A2", "A1"],
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_B1, PidA),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_B1, PidB),
  raft_test_util:assert_leader_commit_index(0, PidA),

  EntriesSplitC_F1 = ["C6", "C5", "C4", "C3", "C2", "C1"],
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F1, PidC),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F1, PidD),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F1, PidE),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitC_F1, PidF),
  raft_test_util:assert_leader_commit_index(6, PidC),

  %%% WHEN : Resolve the split brain
  AllMembers = [PidA, PidB, PidC, PidD, PidE, PidF],
  unset_ignore_peer(AllMembers),
  timer:sleep(300),

  %%% THEN : only one leader
  %%% 1. There is only one leader.
  %%% 2. There are 5 follower.
  %%% 3. There is no candidate.
  %%% 4. All RAFT node should have same entries.
  raft_test_util:assert_node_state_equal_exactly(1, leader, AllMembers),
  raft_test_util:assert_node_state_equal_exactly(0, candidate, AllMembers),
  raft_test_util:assert_node_state_equal_exactly(5, follower, AllMembers),

  ExpectedEntries = ["C6", "C5", "C4", "C3", "C2", "C1"],
  raft_test_util:assert_equal_entries_with_ignoring_term(ExpectedEntries, PidA),
  raft_test_util:assert_member_has_same_entries(AllMembers),
  raft_test_util:assert_leader_commit_index(6, PidA),
  raft_test_util:assert_commit_index_for_members(6, AllMembers),

  %%% After being resolved, `add_entry(...)` request should works well.
  %%% WHEN: Add 3 entries
  raft_api:add_entry_async(PidC, "D1", self()),
  raft_api:add_entry_async(PidC, "D2", self()),
  raft_api:add_entry_async(PidC, "D3", self()),
  timer:sleep(150),

  %%% THEN: Entries should be added
  ExpectedEntries1 = ["D3", "D2", "D1", "C6", "C5", "C4", "C3", "C2", "C1"],
  raft_test_util:assert_equal_entries_with_ignoring_term(ExpectedEntries1, PidA),
  raft_test_util:assert_member_has_same_entries(AllMembers),
  raft_test_util:assert_commit_index_for_members(9, AllMembers),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(RaftMembers).


split_brain2_test() ->
  %%% GIVEN1 -> Raft nodes are given
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D', 'E', 'F'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers, raft_test_factory:default_raft_config()),
  raft_util:set_timer_time(150),
  timer:sleep(30),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidE} = raft_node_state_machine:start('E', RaftMembers, raft_test_factory:default_raft_config()),
  {ok, PidF} = raft_node_state_machine:start('F', RaftMembers, raft_test_factory:default_raft_config()),

  %%% WHEN1 : make Node 'A' leader.
  IgnoreMembers = ['B', 'C', 'D', 'E', 'F'],
  MakeKingMembers = [PidB, PidC, PidD, PidE, PidF],
  set_ignore_msg_from_this_peer(MakeKingMembers, IgnoreMembers),

  timer:sleep(500),
  unset_ignore_peer(MakeKingMembers),

  %%% THEN1 : Node A is leader
  raft_test_util:assert_node_state_equal(leader, PidA),


  %%% WHEN1 - Trigger Split Brain, and send different messages to each split clusters.
  %%% But there is no majority cluster.
  raft_util:set_timer_time(150),

  ClusterA = [PidA, PidB, PidC],
  IgnoreMemberA = ['D', 'E', 'F'],

  ClusterB = [PidD, PidE, PidF],
  IgnoreMemberB = ['A', 'B', 'C'],

  timer:sleep(1500),
  set_ignore_msg_from_this_peer(ClusterA, IgnoreMemberA),
  set_ignore_msg_from_this_peer(ClusterB, IgnoreMemberB),

  raft_api:add_entry_async(PidA, "A1", self()),
  raft_api:add_entry_async(PidA, "A2", self()),

  raft_util:set_timer_time(50),
  timer:sleep(1500),

  raft_api:add_entry_async(PidD, "D1", self()),
  raft_api:add_entry_async(PidD, "D2", self()),

  timer:sleep(50),

  %%% THEN
  %%% 1. Cluster A can have 2 entries, because leader exists. but it cannot commit because there is no majority.
  %%% 2. Cluster B can have no entries, because leader not exists.
  SplitA_C = [PidA, PidB, PidC],
  EntriesSplitA_C0 = ["A2", "A1"],
  raft_test_util:assert_node_state_equal_exactly(1, leader, SplitA_C),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_C0, PidA),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_C0, PidB),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitA_C0, PidC),
  raft_test_util:assert_commit_index_for_members(0, [PidA, PidB, PidC]),

  SplitD_F = [PidD, PidE, PidF],
  EntriesSplitD_F0 = [],
  raft_test_util:assert_node_state_equal_exactly(0, leader, SplitD_F),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitD_F0, PidD),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitD_F0, PidE),
  raft_test_util:assert_equal_entries_with_ignoring_term(EntriesSplitD_F0, PidF),
  raft_test_util:assert_commit_index_for_members(0, [PidD, PidE, PidF]),

  %%% WHEN : Resolve the split brain
  AllMembers = [PidA, PidB, PidC, PidD, PidE, PidF],
  unset_ignore_peer(AllMembers),
  timer:sleep(300),

  %%% THEN : only one leader
  %%% 1. There is only one leader.
  %%% 2. There are 5 follower.
  %%% 3. There is no candidate.
  %%% 4. All RAFT node should have same entries.
  raft_test_util:assert_node_state_equal_exactly(1, leader, AllMembers),
  raft_test_util:assert_node_state_equal_exactly(0, candidate, AllMembers),
  raft_test_util:assert_node_state_equal_exactly(5, follower, AllMembers),
  raft_test_util:assert_member_has_same_entries(AllMembers),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(RaftMembers).


%%%% Util Function.
set_ignore_msg_from_this_peer([], _IgnoreMembers) ->
  ok;
set_ignore_msg_from_this_peer([Pid|Rest], IgnoreMembers) ->
  raft_api:set_ignore_msg_from_this_peer(Pid, IgnoreMembers),
  set_ignore_msg_from_this_peer(Rest, IgnoreMembers).

unset_ignore_peer([]) ->
  ok;
unset_ignore_peer([Pid|Rest]) ->
  raft_api:unset_ignore_peer(Pid),
  unset_ignore_peer(Rest).