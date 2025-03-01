-module(raft_node_state_machine_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").


initialized_test() ->
  %%% WHEN
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% WHEN : Trigger election timeout.
  timer:sleep(300),

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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
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
  raft_util:set_timer_time(150),
  {ok, Pid} = raft_node_state_machine:start('A', ['A']),

  %%% WHEN
  timer:sleep(300),

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
  raft_util:set_timer_time(150),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% WHEN
  timer:sleep(300),
  OlderAppendEntries = raft_rpc_append_entries:new(0, 'B', 0, 0, [], 0),
  gen_statem:cast(whereis('A'), {append_entries, OlderAppendEntries}),

  %%% THEN
  State = raft_node_state_machine:get_state(Pid),
  ?assertEqual(candidate, State),

  %%% CLEAN UP
  raft_node_state_machine:stop('A').


leader_should_send_append_entries_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  timer:sleep(40),
  {ok, PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),

  %%% WHEN -> election timeout can occur at least 5 times.
  timer:sleep(500),

  %%% THEN
  RaftStateA = raft_node_state_machine:get_state(PidA),
  RaftStateB = raft_node_state_machine:get_state(PidB),
  ?assertEqual(leader, RaftStateA),
  ?assertEqual(follower, RaftStateB),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B').

%%%% TODO : Flaky Test
when_client_send_new_entry_to_leader_then_leader_should_keep_it_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  timer:sleep(40),
  {ok, PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),

  timer:sleep(200),

  %%% WHEN -> election timeout can occur at least 5 times.
  Result = raft_node_state_machine:add_entry(PidA, "Hello"),

  %%% THEN
  PidAEntries = raft_node_state_machine:get_log_entries(PidA),

  ?assertEqual(success, Result),
  ?assertEqual([{1, "Hello"}], PidAEntries),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B').


when_client_send_new_entry_to_leader_then_it_should_propagate_to_follower_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),
  {ok, PidC} = raft_node_state_machine:start('C', ['A', 'B', 'C']),

  timer:sleep(200),

  %%% WHEN -> election timeout can occur at least 5 times.
  Result = raft_node_state_machine:add_entry(PidA, "Hello"),

  %%% THEN
  timer:sleep(1000),
  PidBEntries = raft_node_state_machine:get_log_entries(PidB),
  PidCEntries = raft_node_state_machine:get_log_entries(PidC),

  ?assertEqual(success, Result),
  ?assertEqual([{1, "Hello"}], PidBEntries),
  ?assertEqual([{1, "Hello"}], PidCEntries),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B'),
  raft_node_state_machine:stop('C').


when_client_send_two_new_entry_to_leader_then_it_should_propagate_to_follower_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),
  {ok, PidC} = raft_node_state_machine:start('C', ['A', 'B', 'C']),

  timer:sleep(200),

  %%% WHEN -> election timeout can occur at least 5 times.
  Result1 = raft_node_state_machine:add_entry(PidA, "Hello1"),
  timer:sleep(200),
  Result2 = raft_node_state_machine:add_entry(PidA, "Hello2"),

  %%% THEN
  timer:sleep(100),
  PidBEntries = raft_node_state_machine:get_log_entries(PidB),
  PidCEntries = raft_node_state_machine:get_log_entries(PidC),

  ?assertEqual(success, Result1),
  ?assertEqual(success, Result2),
  ?assertEqual([{1, "Hello2"}, {1, "Hello1"}], PidBEntries),
  ?assertEqual([{1, "Hello2"}, {1, "Hello1"}], PidCEntries),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B'),
  raft_node_state_machine:stop('C').


when_client_send_two_new_entry_to_leader_immediately_then_it_should_propagate_to_follower_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),
  {ok, PidC} = raft_node_state_machine:start('C', ['A', 'B', 'C']),

  timer:sleep(200),

  %%% WHEN -> election timeout can occur at least 5 times.
  Result1 = raft_node_state_machine:add_entry(PidA, "Hello1"),
  Result2 = raft_node_state_machine:add_entry(PidA, "Hello2"),

  %%% THEN
  timer:sleep(100),
  PidAEntries = raft_node_state_machine:get_log_entries(PidA),
  PidBEntries = raft_node_state_machine:get_log_entries(PidB),
  PidCEntries = raft_node_state_machine:get_log_entries(PidC),

  ?assertEqual(success, Result1),
  ?assertEqual(success, Result2),
  ?assertEqual([{1, "Hello2"}, {1, "Hello1"}], PidAEntries),
  ?assertEqual([{1, "Hello2"}, {1, "Hello1"}], PidBEntries),
  ?assertEqual([{1, "Hello2"}, {1, "Hello1"}], PidCEntries),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B'),
  raft_node_state_machine:stop('C').


% TODO : InstallSnapshot 시나리오 추가 (노드 중 하나가 오랫동안 다운되었따가 재시작하는 시나리오)


flush_msg_() ->
  receive
    _ -> ok
  after 0 ->
    ok
  end.

