-module(raft_node_state_machine_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").


initialized_test() ->
  %%% WHEN
  raft_util:set_timer_time(150),
  {ok, Pid} = raft_node_state_machine:start('A', ['A', 'B', 'C']),

  %%% THEN
  State = raft_api:get_state(Pid),
  CurrentTerm = raft_api:get_current_term(Pid),
  VotedCount = raft_api:get_voted_count(Pid),

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
  State = raft_api:get_state(Pid),
  CurrentTerm = raft_api:get_current_term(Pid),
  VotedCount = raft_api:get_voted_count(Pid),

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
  PreviousTimer = raft_api:get_timer(Pid),
  erlang:register('B', self()),
  ValidTermId = 10,

  %%% WHEN
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', ValidTermId, 10, 10),
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTimer = raft_api:get_timer(Pid),
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
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

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
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTerm = raft_api:get_current_term(Pid),
  VotedFor = raft_api:get_voted_for(Pid),

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
  PreviousTimer = raft_api:get_timer(Pid),

  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 10, 10),
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

  %%% THEN
  CurrentTimer = raft_api:get_timer(Pid),
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

  CurrentTerm = raft_api:get_current_term(Pid),
  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 20, 30),
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

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

  CurrentTerm = raft_api:get_current_term(Pid),
  erlang:register('B', self()),

  %%% WHEN
  InvalidTermId = -1,
  NewRequestVoted = raft_rpc_request_vote:new_request_vote('B', InvalidTermId, 20, 30),
  raft_rpc_request_vote:request_vote(Pid, NewRequestVoted),

  %%% THEN
  AfterCurrentTerm = raft_api:get_current_term(Pid),
  AfterVotedFor = raft_api:get_voted_for(Pid),

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
  State = raft_api:get_state(Pid),
  CurrentTerm = raft_api:get_current_term(Pid),
  VotedCount = raft_api:get_voted_count(Pid),

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
  State = raft_api:get_state(Pid),
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
  RaftStateA = raft_api:get_state(PidA),
  RaftStateB = raft_api:get_state(PidB),
  ?assertEqual(leader, RaftStateA),
  ?assertEqual(follower, RaftStateB),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B').

when_client_send_new_entry_to_leader_then_leader_should_keep_it_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  {ok, PidA} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
  timer:sleep(40),
  {ok, _PidB} = raft_node_state_machine:start('B', ['A', 'B', 'C']),

  timer:sleep(200),

  %%% WHEN -> election timeout can occur at least 5 times.
  raft_api:add_entry_async(PidA, "Hello", self()),

  %%% THEN
  ExpectedEntries = [{1, "Hello"}],
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidA),

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
  raft_api:add_entry_async(PidA, "Hello", self()),

  %%% THEN
  timer:sleep(1000),
  ExpectedEntries = [{1, "Hello"}],
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),


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
  raft_api:add_entry_async(PidA, "Hello1", self()),
  timer:sleep(200),
  raft_api:add_entry_async(PidA, "Hello2", self()),

  %%% THEN
  timer:sleep(100),
  ExpectedEntries = [{1, "Hello2"}, {1, "Hello1"}],
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),

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
  raft_api:add_entry_async(PidA, "Hello1", self()),
  raft_api:add_entry_async(PidA, "Hello2", self()),

  %%% THEN
  timer:sleep(100),
  ExpectedEntries = [{1, "Hello2"}, {1, "Hello1"}],

  raft_test_util:assert_entries_node_has(ExpectedEntries, PidA),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),

  %%% CLEAN UP
  raft_node_state_machine:stop('A'),
  raft_node_state_machine:stop('B'),
  raft_node_state_machine:stop('C').


add_entry_redirect_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  %%% WHEN
  timer:sleep(200),
  raft_api:add_entry_async(PidA, "MsgToA", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidB, "MsgToB", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidC, "MsgToC", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidC, "MsgToD", self()),

  %%% THEN
  %%% Wait until commit
  timer:sleep(20),
  ExpectedEntries = [{1, "MsgToD"}, {1, "MsgToC"}, {1, "MsgToB"}, {1, "MsgToA"}],

  raft_test_util:assert_entries_node_has(ExpectedEntries, PidA),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidD),

  safe_stop_nodes(RaftMembers).


leader_goes_down_and_new_leader_is_elected_and_previous_leader_can_be_follower_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN1
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  %%% WHEN1
  timer:sleep(200),

  %%% THEN1
  ?assertEqual(leader, raft_api:get_state(PidA)),

  %%% GIVEN2
  raft_node_state_machine:stop('A'),

  %%% WHEN2
  timer:sleep(200),

  PidBState = raft_api:get_state(PidB),
  PidCState = raft_api:get_state(PidC),
  PidDState = raft_api:get_state(PidD),
  LeaderElected = lists:member(leader, [PidBState, PidCState, PidDState]),
  ?assertEqual(true, LeaderElected),

  %%% WHEN3
  {ok, NewPidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(200),

  %%% THEN3
  PidAState = raft_api:get_state(NewPidA),
  ?assertEqual(follower, PidAState),

  %%% WHEN4
  raft_api:add_entry_async(NewPidA, "MsgToA", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidB, "MsgToB", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidC, "MsgToC", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidD, "MsgToD", self()),

  %%% THEN4
  %%% Wait until commit
  timer:sleep(20),
  ExpectedEntries = [{2, "MsgToD"}, {2, "MsgToC"}, {2, "MsgToB"}, {2, "MsgToA"}],

  raft_test_util:assert_entries_node_has(ExpectedEntries, NewPidA),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidD),

  safe_stop_nodes(RaftMembers).


leader_goes_down_and_new_leader_is_elected_and_previous_leader_can_restore_logs_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN1
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  %%% WHEN1
  timer:sleep(200),
  raft_api:add_entry_async(PidA, "MsgToA", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidB, "MsgToB", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidC, "MsgToC", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidD, "MsgToD", self()),

  %%% THEN1
  timer:sleep(20),
  ExpectedEntries = [{1, "MsgToD"}, {1, "MsgToC"}, {1, "MsgToB"}, {1, "MsgToA"}],

  ?assertEqual(leader, raft_api:get_state(PidA)),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidA),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidD),

  %%% GIVEN2
  raft_node_state_machine:stop('A'),

  %%% WHEN2
  timer:sleep(200),

  PidBState = raft_api:get_state(PidB),
  PidCState = raft_api:get_state(PidC),
  PidDState = raft_api:get_state(PidD),
  LeaderElected = lists:member(leader, [PidBState, PidCState, PidDState]),
  ?assertEqual(true, LeaderElected),

  %%% WHEN3
  {ok, NewPidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(200),

  %%% THEN3
  PidAState = raft_api:get_state(NewPidA),
  ?assertEqual(follower, PidAState),
  raft_test_util:assert_entries_node_has(ExpectedEntries, NewPidA),

  safe_stop_nodes(RaftMembers).



leader_goes_down_and_new_leader_is_elected_and_multi_node_goes_down_and_restore_logs_eventually_test() ->
  %%% SETUP
  flush_msg_(),

  %%% GIVEN1
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  timer:sleep(50),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),

  %%% WHEN1
  timer:sleep(200),
  raft_api:add_entry_async(PidA, "MsgToA", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidB, "MsgToB", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidC, "MsgToC", self()),
  timer:sleep(20),
  raft_api:add_entry_async(PidD, "MsgToD", self()),

  %%% THEN1
  timer:sleep(20),
  ExpectedEntries = [{1, "MsgToD"}, {1, "MsgToC"}, {1, "MsgToB"}, {1, "MsgToA"}],

  ?assertEqual(leader, raft_api:get_state(PidA)),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidA),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidB),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidC),
  raft_test_util:assert_entries_node_has(ExpectedEntries, PidD),

  %%% WHEN2 + THEN2
  assert_entries_restore_when_stop_and_restart_node('A', ExpectedEntries, RaftMembers),
  assert_entries_restore_when_stop_and_restart_node('B', ExpectedEntries, RaftMembers),
  assert_entries_restore_when_stop_and_restart_node('C', ExpectedEntries, RaftMembers),
  assert_entries_restore_when_stop_and_restart_node('D', ExpectedEntries, RaftMembers),

  safe_stop_nodes(RaftMembers).


% TODO : InstallSnapshot 시나리오 추가 (노드 중 하나가 오랫동안 다운되었따가 재시작하는 시나리오)
% TODO : append_entries 결과를 받았을 때, Commit 올라가는 것을 확인.
% TODO : Split vote 확인 및 해소되는 것 확인.

flush_msg_() ->
  receive
    _ -> ok
  after 0 ->
    ok
  end.

safe_stop_nodes([]) ->
  ok;
safe_stop_nodes([Head|Rest]) ->
  try
      raft_node_state_machine:stop(Head)
  catch
      _:_  -> ok
  end,
  safe_stop_nodes(Rest).

assert_entries_restore_when_stop_and_restart_node(NodeName, ExpectedEntries, RaftMembers) ->
  raft_node_state_machine:stop(NodeName),

  %%% WHEN3
  {ok, Pid} = raft_node_state_machine:start(NodeName, RaftMembers),
  timer:sleep(200),
  raft_test_util:assert_entries_node_has(ExpectedEntries, Pid).