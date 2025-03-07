-module(raft_test_util).

%% API
-include_lib("eunit/include/eunit.hrl").

-export([assert_entries_node_has/2]).
-export([assert_node_state_equal/2]).
-export([assert_node_state_not_equal/2]).
-export([assert_node_term_grater_than/2]).
-export([assert_node_state_equal_exactly/3]).
-export([assert_members_equal_exactly/3]).
-export([flush_msg_/0]).
-export([safe_stop_nodes/1]).


-type raft_term() :: integer().

-type data() :: any().

-type entry() :: {raft_term(), data()}.


-spec assert_entries_node_has(ExpectedEntries, NodePid) -> ok when
  ExpectedEntries :: [entry()],
  NodePid :: pid().
assert_entries_node_has(ExpectedEntries, NodePid) ->
  EntriesActually = raft_api:get_log_entries(NodePid),
  ?assertEqual(ExpectedEntries, EntriesActually).


assert_node_state_equal(ExpectedState, NodePid) ->
  State = raft_api:get_state(NodePid),
  ?assertEqual(ExpectedState, State).

assert_node_state_not_equal(ExpectedState, NodePid) ->
  State = raft_api:get_state(NodePid),
  ?assertNotEqual(ExpectedState, State).

assert_node_term_grater_than(NodeTerm, NodePid) ->
  FindNodeTerm = raft_api:get_current_term(NodePid),
  ?assert(NodeTerm < FindNodeTerm).


assert_node_state_equal_exactly(ExpectedCount, ExpectedState, Pids) ->
  TotalCount = assert_node_state_equal_exactly_(ExpectedState, Pids, 0),
  ?assertEqual(ExpectedCount, TotalCount).

assert_node_state_equal_exactly_(_ExpectedState, [], AccCount) ->
  AccCount;
assert_node_state_equal_exactly_(ExpectedState, [Pid|Rest], AccCount0) ->
  State = raft_api:get_state(Pid),
  case State =:= ExpectedState of
    true -> assert_node_state_equal_exactly_(ExpectedState, Rest, AccCount0+1);
    false -> assert_node_state_equal_exactly_(ExpectedState, Rest, AccCount0)
  end.

assert_members_equal_exactly(ExpectedNewMembers, ExpectedOldMembers, Pid) ->
  ActualNewMembers = sets:to_list(raft_api:get_new_members(Pid)),
  ActualOldMembers = sets:to_list(raft_api:get_old_members(Pid)),
  ?assertEqual(ExpectedNewMembers, ActualNewMembers),
  ?assertEqual(ExpectedOldMembers, ActualOldMembers).


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