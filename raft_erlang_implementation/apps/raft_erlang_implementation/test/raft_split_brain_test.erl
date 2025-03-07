-module(raft_split_brain_test).

-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).


split_brain_test() ->
  %%% GIVEN -> NodeA is leader.
  raft_util:set_timer_time(50),
  RaftMembers = ['A', 'B', 'C', 'D', 'E', 'F'],
  {ok, PidA} = raft_node_state_machine:start('A', RaftMembers),
  raft_util:set_timer_time(150),
  timer:sleep(30),
  {ok, PidB} = raft_node_state_machine:start('B', RaftMembers),
  {ok, PidC} = raft_node_state_machine:start('C', RaftMembers),
  {ok, PidD} = raft_node_state_machine:start('D', RaftMembers),
  {ok, PidE} = raft_node_state_machine:start('E', RaftMembers),
  {ok, PidF} = raft_node_state_machine:start('F', RaftMembers),

  IgnoreMembers = ['B', 'C', 'D', 'E', 'F'],

  raft_api:set_ignore_msg_from_this_peer(PidB, IgnoreMembers),
  raft_api:set_ignore_msg_from_this_peer(PidC, IgnoreMembers),
  raft_api:set_ignore_msg_from_this_peer(PidD, IgnoreMembers),
  raft_api:set_ignore_msg_from_this_peer(PidE, IgnoreMembers),
  raft_api:set_ignore_msg_from_this_peer(PidF, IgnoreMembers),

  timer:sleep(500),
  raft_test_util:assert_node_state_equal(leader, PidA),

  raft_api:unset_ignore_peer(PidB),
  raft_api:unset_ignore_peer(PidC),
  raft_api:unset_ignore_peer(PidD),
  raft_api:unset_ignore_peer(PidE),
  raft_api:unset_ignore_peer(PidF),

  %%% WHEN - Trigger Split Brain
  raft_util:set_timer_time(50),
  SplitMembers1 = ['C', 'D', 'E', 'F'],
  SplitMembers2 = ['A', 'B'],

  raft_api:set_ignore_msg_from_this_peer(PidA, SplitMembers1),
  raft_api:set_ignore_msg_from_this_peer(PidB, SplitMembers1),

  raft_api:set_ignore_msg_from_this_peer(PidC, SplitMembers2),
  raft_api:set_ignore_msg_from_this_peer(PidD, SplitMembers2),
  raft_api:set_ignore_msg_from_this_peer(PidE, SplitMembers2),
  raft_api:set_ignore_msg_from_this_peer(PidF, SplitMembers2),


  %%%
  timer:sleep(1500),
  raft_api:add_entry_async(PidA, "A1", self()),
  raft_api:add_entry_async(PidA, "A2", self()),

  raft_api:add_entry_async(PidC, "C1", self()),
  raft_api:add_entry_async(PidC, "C2", self()),

  timer:sleep(50),

  EntriesA = raft_api:get_log_entries(PidA),
  EntriesB = raft_api:get_log_entries(PidB),
  EntriesC = raft_api:get_log_entries(PidC),
  EntriesD = raft_api:get_log_entries(PidD),
  EntriesE = raft_api:get_log_entries(PidE),
  EntriesF = raft_api:get_log_entries(PidF),


  io:format("
  StateA: ~p
  StateB: ~p
  StateC: ~p
  StateD: ~p
  StateE: ~p
  StateF: ~p~n", [
    raft_api:get_state(PidA),
    raft_api:get_state(PidB),
    raft_api:get_state(PidC),
    raft_api:get_state(PidD),
    raft_api:get_state(PidE),
    raft_api:get_state(PidF)
  ]),


  io:format("
  EntriesA: ~p,
  EntriesB: ~p,
  EntriesC: ~p,
  EntriesD: ~p,
  EntriesE: ~p,
  EntriesF: ~p~n", [EntriesA, EntriesB, EntriesC, EntriesD, EntriesE, EntriesF]),

%%  ?assertEqual(1, 2),

  %%% CLEAN UP
  raft_util:clean_up_timer_time(),
  raft_test_util:safe_stop_nodes(RaftMembers).

