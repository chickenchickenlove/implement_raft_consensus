-module(raft_node_state_machine_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").


initialized_test() ->
  %%% WHEN
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(follower, State),
  ?assertEqual(0, CurrentTerm),
  ?assertEqual(0, VotedCount),


  erlang:unregister('A'),

  timer:sleep(500).


follow_turns_to_candidate_test() ->
  %%% GIVEN
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% WHEN
  timer:sleep(500),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(candidate, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(1, VotedCount),

  erlang:unregister('A'),

  timer:sleep(500).


follow_turns_to_leader_immediately_if_alone_test() ->
  %%% GIVEN
  {ok, Pid} = raft_node_state_machine:start('A', ['A']),

  %%% WHEN
  timer:sleep(500),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(leader, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(1, VotedCount),

  erlang:unregister('A'),

  timer:sleep(500).
