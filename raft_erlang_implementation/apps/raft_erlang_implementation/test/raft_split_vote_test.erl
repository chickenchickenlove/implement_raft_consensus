-module(raft_split_vote_test).

-export([]).

-include_lib("eunit/include/eunit.hrl").

split_vote_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  RaftMembers = ['A', 'B', 'C', 'D'],
  raft_util:set_timer_time(50),

  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidA, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidB, ['A', 'D']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['A', 'D']),
  raft_api:set_ignore_msg_from_this_peer(PidD, ['B', 'C']),

  %%% WHEN
  timer:sleep(200),

  %%% THEN
  Pids = [PidA, PidB, PidC, PidD],
  raft_test_util:assert_node_state_equal_exactly(0, leader, Pids),

  raft_test_util:assert_node_term_grater_than(1, PidA),
  raft_test_util:assert_node_term_grater_than(1, PidB),
  raft_test_util:assert_node_term_grater_than(1, PidC),
  raft_test_util:assert_node_term_grater_than(1, PidD),

  %%% CLEAN UP
  raft_test_util:safe_stop_nodes(RaftMembers).


split_vote_and_eventually_resolved_test() ->
  %%% SETUP
  raft_test_util:flush_msg_(),

  %%% GIVEN
  RaftMembers = ['A', 'B', 'C', 'D'],
  raft_util:set_timer_time(50),

  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  raft_api:set_ignore_msg_from_this_peer(PidA, ['B', 'C']),
  raft_api:set_ignore_msg_from_this_peer(PidB, ['A', 'D']),
  raft_api:set_ignore_msg_from_this_peer(PidC, ['A', 'D']),
  raft_api:set_ignore_msg_from_this_peer(PidD, ['B', 'C']),

  %%% WHEN1
  timer:sleep(200),

  %%% THEN1
  Pids = [PidA, PidB, PidC, PidD],
  raft_test_util:assert_node_state_equal_exactly(0, leader, Pids),

  %%% GIVEN2
  raft_api:unset_ignore_peer(PidA),
  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),
  raft_api:unset_ignore_peer(PidD),

  %%% WHEN2
  timer:sleep(200),

  %%% THEN2
  raft_test_util:assert_node_state_equal_exactly(1, leader, Pids),
  raft_test_util:assert_node_state_equal_exactly(3, follower, Pids),


  %%% CLEAN UP
  raft_test_util:safe_stop_nodes(RaftMembers).
