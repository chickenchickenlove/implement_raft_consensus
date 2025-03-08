-module(raft_joint_consensus_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").

joint_consensus_test() ->
  %%% GIVEN -> NodeA is leader.
  raft_util:set_timer_time(100),
  OldRaftMembers = ['A', 'B', 'C'],
  NewRaftMembers = ['A', 'D', 'E'],
  {ok, PidA} = raft_node_state_machine:start('A', OldRaftMembers),
  timer:sleep(80),
  {ok, PidB} = raft_node_state_machine:start('B', OldRaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', OldRaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidB, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['B', 'C']),

  timer:sleep(500),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),


  %%% WHEN1 -> Start new membership and prepare.
  {ok, PidD} = raft_node_state_machine:start('D', NewRaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', NewRaftMembers),
  raft_api:prepare_new_cluster_membership(PidA, NewRaftMembers),

  %%% THEN1
  timer:sleep(200),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidA),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidB),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidC),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidD),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidE),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(OldRaftMembers),
  raft_test_util:safe_stop_nodes(NewRaftMembers).


joint_consensus1_test() ->

  %%% GIVEN -> NodeA is leader and RAFT Cluster is in Cold+new status.
  raft_util:set_timer_time(100),
  OldRaftMembers = ['A', 'B', 'C'],
  NewRaftMembers = ['A', 'D', 'E'],
  {ok, PidA} = raft_node_state_machine:start('A', OldRaftMembers),
  timer:sleep(80),
  {ok, PidB} = raft_node_state_machine:start('B', OldRaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', OldRaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidB, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['B', 'C']),

  timer:sleep(1000),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),

  {ok, PidD} = raft_node_state_machine:start('D', NewRaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', NewRaftMembers),

  raft_api:prepare_new_cluster_membership(PidA, NewRaftMembers),

  timer:sleep(200),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidA),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidB),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidC),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidD),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidE),

  %%% WHEN - add new entry
  raft_api:add_entry_async(PidA, "A1", self()),

  %%% THEN
  timer:sleep(200),
  ExpectedEntries = [{1,"A1"},{1,{new_membership,['A','D','E'],['A','B','C']}}],
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidA)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidB)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidC)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidD)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidE)),

  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(OldRaftMembers),
  raft_test_util:safe_stop_nodes(NewRaftMembers).


joint_consensus2_test() ->
  %%% GIVEN1 -> NodeA is leader and RAFT Cluster is in Cold+new status.
  raft_util:set_timer_time(100),
  OldRaftMembers = ['A', 'B', 'C'],
  NewRaftMembers = ['A', 'D', 'E'],
  {ok, PidA} = raft_node_state_machine:start('A', OldRaftMembers),
  timer:sleep(80),
  {ok, PidB} = raft_node_state_machine:start('B', OldRaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', OldRaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidB, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['B', 'C']),

  timer:sleep(500),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),

  %%% WHEN1 -> RAFT cluster get A1 entry and prepare new membership.
  raft_api:add_entry_async(PidA, "A1", self()),
  {ok, PidD} = raft_node_state_machine:start('D', NewRaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', NewRaftMembers),
  raft_api:prepare_new_cluster_membership(PidA, NewRaftMembers),

  timer:sleep(300),

  %%% THEN1 -> All Old and New RaftMembers has Cold+new members as their members.
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidA),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidB),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidC),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidD),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidE),

  %%% WHEN2 -> RAFT cluster get A2 entry in Cold+new State.
  raft_api:add_entry_async(PidA, "A2", self()),
  timer:sleep(200),

  %%% THEN2 -> RAFT cluster should get A1, A2 entry and new_membership RPC as well.
  ExpectedEntries = [{1,"A2"}, {1,{new_membership,['A','D','E'],['A','B','C']}}, {1,"A1"}],
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidA)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidB)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidC)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidD)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidE)),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(OldRaftMembers),
  raft_test_util:safe_stop_nodes(NewRaftMembers).


joint_consensus4_test() ->
  %%% GIVEN -> NodeA is leader and RAFT Cluster is in Cold+new status.
  raft_util:set_timer_time(100),
  OldRaftMembers = ['A', 'B', 'C'],
  NewRaftMembers = ['A', 'D', 'E'],

  {ok, PidA} = raft_node_state_machine:start('A', OldRaftMembers),
  timer:sleep(80),
  {ok, PidB} = raft_node_state_machine:start('B', OldRaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', OldRaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidB, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['B', 'C']),

  timer:sleep(500),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),

  {ok, PidD} = raft_node_state_machine:start('D', NewRaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', NewRaftMembers),

  raft_api:prepare_new_cluster_membership(PidA, NewRaftMembers),

  timer:sleep(500),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidA),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidB),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidC),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidD),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidE),

  %%% WHEN : confirm new membership
  raft_api:confirm_new_cluster_membership(PidA, NewRaftMembers),

  %%% Then : Old membership should be deprecated.
  timer:sleep(200),
  PidList = [PidA, PidB, PidC, PidD, PidE],
  raft_test_util:assert_node_state_equal_exactly(1, leader, PidList),
  raft_test_util:assert_node_state_equal_exactly(2, follower, PidList),
  raft_test_util:assert_node_state_equal_exactly(2, deprecated, PidList),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(OldRaftMembers),
  raft_test_util:safe_stop_nodes(NewRaftMembers).


joint_consensus5_test() ->
  %%% GIVEN -> NodeA is leader and RAFT Cluster is in Cold+new status, then has been confirmed new membership.
  raft_util:set_timer_time(100),
  OldRaftMembers = ['A', 'B', 'C'],
  NewRaftMembers = ['A', 'D', 'E'],
  {ok, PidA} = raft_node_state_machine:start('A', OldRaftMembers),
  timer:sleep(90),
  {ok, PidB} = raft_node_state_machine:start('B', OldRaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', OldRaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidB, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['B', 'C']),

  timer:sleep(500),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),

  {ok, PidD} = raft_node_state_machine:start('D', NewRaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', NewRaftMembers),

  raft_api:prepare_new_cluster_membership(PidA, NewRaftMembers),

  timer:sleep(300),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidA),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidB),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidC),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidD),
  raft_test_util:assert_members_equal_exactly(NewRaftMembers, OldRaftMembers, PidE),

  raft_api:confirm_new_cluster_membership(PidA, NewRaftMembers),

  timer:sleep(200),
  PidList = [PidA, PidB, PidC, PidD, PidE],
  raft_test_util:assert_node_state_equal_exactly(1, leader, PidList),
  raft_test_util:assert_node_state_equal_exactly(2, follower, PidList),
  raft_test_util:assert_node_state_equal_exactly(2, deprecated, PidList),

  %%% WHEN : Send 4 entries to RAFT Membership.
  raft_api:add_entry_async(PidA, "A1", self()),
  raft_api:add_entry_async(PidA, "A2", self()),
  raft_api:add_entry_async(PidA, "A3", self()),
  raft_api:add_entry_async(PidA, "A4", self()),

  timer:sleep(100),
  ExpectedEntries = [{1,"A4"}, {1,"A3"}, {1,"A2"}, {1,"A1"},
                     {1,{confirm_new_membership,['A','D','E']}},
                     {1,{new_membership,['A','D','E'],['A','B','C']}}
  ],

  ExpectedEntriesDeprecated = [{1,{confirm_new_membership,['A','D','E']}},
                               {1,{new_membership,['A','D','E'],['A','B','C']}}],

  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidA)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidD)),
  ?assertEqual(ExpectedEntries, raft_api:get_log_entries(PidE)),

  ?assertEqual(ExpectedEntriesDeprecated, raft_api:get_log_entries(PidB)),
  ?assertEqual(ExpectedEntriesDeprecated, raft_api:get_log_entries(PidC)),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(OldRaftMembers),
  raft_test_util:safe_stop_nodes(NewRaftMembers).
