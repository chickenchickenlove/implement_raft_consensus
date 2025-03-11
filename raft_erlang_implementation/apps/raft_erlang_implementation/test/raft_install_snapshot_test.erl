-module(raft_install_snapshot_test).

-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([]).


install_snapshot1_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN1
  RaftConfig = raft_test_factory:raft_config([{max_log_size, 5}, {interval_of_install_snapshot, 100}]),
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D'],

  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers, RaftConfig),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers, RaftConfig),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers, RaftConfig),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers, RaftConfig),

  %%% WHEN1
  timer:sleep(500),

  %%% THEN1 - leader should be elected.
  raft_test_util:assert_node_state_equal_exactly(1, leader, [PidA, PidB, PidC, PidD]),
  raft_test_util:assert_node_state_equal_exactly(3, follower, [PidA, PidB, PidC, PidD]),

  %%% GIVEN2 - send raft commands.
  raft_api:add_entry_async(PidA, raft_command:set(a, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(b, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(c, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(d, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(e, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(f, 10), self()),

  %%% WHEN2 - Log Compaction is triggered because max logs size configured as 5 and we sent six commands.
  timer:sleep(500),

  %%% THEN2 -  All Compacted
  ?assertEqual([], raft_api:get_log_entries(PidA)),
  ?assertEqual([], raft_api:get_log_entries(PidB)),
  ?assertEqual([], raft_api:get_log_entries(PidC)),
  ?assertEqual([], raft_api:get_log_entries(PidD)),

  ExpectedLocalRaftState = #{a=>10, b=>10, c=>10, d=>10, e=>10, f=>10},
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromA} = raft_api:get_snapshot(PidA),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromB} = raft_api:get_snapshot(PidB),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromC} = raft_api:get_snapshot(PidC),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromD} = raft_api:get_snapshot(PidD),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromA),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromB),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromC),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromD),

  %%% CLEAN UP
  raft_test_util:safe_stop_nodes(RaftMembers).


install_snapshot2_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN1
  raft_util:set_timer_time(50),
  RaftConfig = raft_test_factory:raft_config([{max_log_size, 5}, {interval_of_install_snapshot, 100}]),
  RaftMembers = ['A', 'B', 'C', 'D'],

  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers, RaftConfig),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers, RaftConfig),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers, RaftConfig),

  %%% WHEN1
  timer:sleep(500),

  %%% THEN1 - leader should be elected.
  raft_test_util:assert_node_state_equal_exactly(1, leader, [PidA, PidB, PidC]),
  raft_test_util:assert_node_state_equal_exactly(2, follower, [PidA, PidB, PidC]),

  %%% GIVEN2 - send raft commands.
  raft_api:add_entry_async(PidA, raft_command:set(a, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(b, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(c, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(d, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(e, 10), self()),
  raft_api:add_entry_async(PidA, raft_command:set(f, 10), self()),

  %%% WHEN2 - wait for log to be compacted.
  timer:sleep(500),

  %%% THEN2 - log compaction was performed sucessufully.
  ?assertEqual([], raft_api:get_log_entries(PidA)),
  ?assertEqual([], raft_api:get_log_entries(PidB)),
  ?assertEqual([], raft_api:get_log_entries(PidC)),

  ExpectedLocalRaftState = #{a=>10, b=>10, c=>10, d=>10, e=>10, f=>10},
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromA} = raft_api:get_snapshot(PidA),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromB} = raft_api:get_snapshot(PidB),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromC} = raft_api:get_snapshot(PidC),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromA),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromB),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromC),

  %%% GIVEN3 - New Node starts after being completed log compaction.
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers, RaftConfig),

  %%% WHEN3 - Wait for install_snapshot RPC
  timer:sleep(200),

  %%% THEN3 - new node should catch up all logs
  ?assertEqual([], raft_api:get_log_entries(PidD)),
  #raft_snapshot{local_raft_state=ActualLocalRaftStateFromD} = raft_api:get_snapshot(PidD),
  ?assertEqual(ExpectedLocalRaftState, ActualLocalRaftStateFromD),

  %%% CLEAN UP
  raft_test_util:safe_stop_nodes(RaftMembers).