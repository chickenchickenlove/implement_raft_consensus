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

  %%% WHEN : Trigger election timeout.
  timer:sleep(650),

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


follower_should_refresh_its_timer_when_valid_candidate_sends_a_request_vote_rpc_test() ->
  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  PreviousTimer = raft_node_state_machine:get_timer(Pid),
  erlang:register('B', self()),
  ValidTermId = 10,

  %%% WHEN
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', ValidTermId, 10, 10),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTimer = raft_node_state_machine:get_timer(Pid),
  ?assertNotEqual(PreviousTimer, CurrentTimer),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').

follower_should_ack_vote_when_valid_candidate_sends_a_request_vote_rpc_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  erlang:register('B', self()),

  %%% WHEN
  ValidTermId = 10,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', ValidTermId, 10, 10),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  AckVotedMsg = receive
                  Msg -> Msg
                end,
  {'$gen_cast',{ack_request_voted, VotedNodeName, VotedNodeCurrentTerm, VoteGranted}} = AckVotedMsg,

  ?assertEqual(VotedNodeName, 'A'),
  ?assertEqual(VotedNodeCurrentTerm, 10),
  ?assertEqual(VoteGranted, true),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').

follower_should_update_its_state_when_valid_candidate_sends_a_request_vote_rpc_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  erlang:register('B', self()),

  %%% WHEN
  ValidTermId = 10,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', ValidTermId, 10, 10),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedFor = raft_node_state_machine:get_voted_for(Pid),

  ?assertEqual(ValidTermId, CurrentTerm),
  ?assertEqual('B', VotedFor),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


follower_should_not_refresh_its_timer_when_invalid_candidate_sends_a_requested_rpc_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  PreviousTimer = raft_node_state_machine:get_timer(Pid),

  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 10, 10),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTimer = raft_node_state_machine:get_timer(Pid),
  ?assertEqual(PreviousTimer, CurrentTimer),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


follower_should_not_voted_when_invalid_candidate_sends_a_request_vote_rpc_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 20, 30),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  AckVotedMsg = receive
                  Msg -> Msg
                end,
  {'$gen_cast',{ack_request_voted, VotedNodeName, VotedNodeCurrentTerm, VoteGranted}} = AckVotedMsg,

  ?assertEqual(VotedNodeName, 'A'),
  ?assertEqual(VotedNodeCurrentTerm, CurrentTerm),
  ?assertEqual(VoteGranted, false),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


follower_should_not_update_its_state_when_invalid_candidate_sends_a_request_vote_rpc_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 20, 30),
  raft_node_state_machine:request_vote(Pid, NewRequestVoted),

  %%% THEN
  AfterCurrentTerm = raft_node_state_machine:get_current_term(Pid),
  AfterVotedFor = raft_node_state_machine:get_voted_for(Pid),

  ?assertEqual(CurrentTerm, AfterCurrentTerm),
  ?assertEqual(undefined, AfterVotedFor),

  %%% CLEAN UP
  unregister('B'),
  raft_util:clean_up_timer_time(),
  raft_node_state_machine:stop('A').


candidate_turns_to_leader_immediately_if_alone_in_cluster_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A']),

  %%% WHEN
  timer:sleep(800),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  CurrentTerm = raft_node_state_machine:get_current_term(Pid),
  VotedCount = raft_node_state_machine:get_voted_count(Pid),

  ?assertEqual(leader, State),
  ?assertEqual(1, CurrentTerm),
  ?assertEqual(1, VotedCount),

  raft_node_state_machine:stop('A').


candidate_should_ignore_append_entries_with_older_term_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(500),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% WHEN
  timer:sleep(650),
  OlderAppendEntries = raft_rpc_append_entries:new_append_entries_rpc(0, 'B', 0, 0, [], 0),
  gen_statem:cast(whereis('A'), {append_entries, OlderAppendEntries}),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  ?assertEqual(candidate, State),

  %%% CLEAN UP
  raft_node_state_machine:stop('A').



%%raft_node_state_machine:start('A', ['A', 'B', 'C']),
%%raft_node_state_machine:start('B', ['A', 'B', 'C'])


flush_msg_() ->
  receive
    _ -> ok
  after 0 ->
    ok
  end.

