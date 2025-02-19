-module(raft_node_state_machine_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").


initialized_test() ->
  %%% WHEN
  raft_util:set_timer_time(1000),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(follower, State),
  ?assertEqual(0, CurrentTerm),
  ?assertEqual(0, VotedCount),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


follow_turns_to_candidate_when_election_timeout_occur_test() ->
  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% WHEN
  timer:sleep(800),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(candidate, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(1, VotedCount),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


candidate_turns_to_leader_immediately_if_alone_in_cluster_test() ->
  %%% GIVEN
  {ok, Pid} = raft_node_state_machine:start('A', ['A']),

  %%% WHEN
  raft_util:set_timer_time(500),
  timer:sleep(800),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(leader, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(1, VotedCount),

  raft_node_state_machine:stop('A').

%%% NOT ACTUALLY Implemented.
%%% NODE A vote to itself, Node B also do.
candidate_become_leader_eventually_after_split_vote_test() ->
  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  {ok, Pid} = raft_node_state_machine:start('B', ['A', 'B', 'C']),

  %%% WHEN
  timer:sleep(1000),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(leader, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(2, VotedCount),

  raft_node_state_machine:stop('B'),
  raft_node_state_machine:stop('A').


%%raft_node_state_machine:start('A', ['A', 'B', 'C']),
%%raft_node_state_machine:start('B', ['A', 'B', 'C'])