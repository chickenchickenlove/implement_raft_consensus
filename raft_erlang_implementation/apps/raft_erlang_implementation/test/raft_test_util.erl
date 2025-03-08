-module(raft_test_util).

%% API
-include_lib("eunit/include/eunit.hrl").

-export([assert_member_has_same_entries/1]).
-export([assert_commit_index_for_members/2]).
-export([assert_leader_commit_index/2]).
-export([assert_equal_entries_with_ignoring_term/2]).
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

assert_equal_entries_with_ignoring_term(ExpectedEntriesWithoutTerm, Pid) ->
  Entries = raft_api:get_log_entries(Pid),
  ParsedEntries = parse_(Entries),
  ?assertEqual(ExpectedEntriesWithoutTerm, ParsedEntries).

assert_leader_commit_index(ExpectedCommitIndex, Pid) ->
  LeaderPid = raft_api:get_leader_pid(Pid),
  ActualCommitIndex = raft_api:get_commit_index(LeaderPid),
  ?assertEqual(ExpectedCommitIndex, ActualCommitIndex).

assert_commit_index_for_members(ExpectedCommitIndex, []) ->
  ok;
assert_commit_index_for_members(ExpectedCommitIndex, [Pid|Rest]) ->
  ActualCommitIndex = raft_api:get_commit_index(Pid),
  ?assertEqual(ExpectedCommitIndex, ActualCommitIndex),
  assert_commit_index_for_members(ExpectedCommitIndex, Rest).

assert_member_has_same_entries(RaftMemberPids) ->
  EntriesSet = assert_member_has_same_entries_(RaftMemberPids, sets:new()),
  ?assertEqual(1, sets:size(EntriesSet)).

assert_member_has_same_entries_([], Acc) ->
  Acc;
assert_member_has_same_entries_([Pid|Rest], Acc0) ->
  Entries = raft_api:get_log_entries(Pid),
  Acc = sets:add_element(Entries, Acc0),
  assert_member_has_same_entries_(Rest, Acc).

parse_(Entries) ->
  parse_(Entries, []).

parse_([], Acc) ->
  lists:reverse(Acc);
parse_([Head|Rest]=_Entries, Acc) ->
  {_Term, Data} = Head,
  parse_(Rest, [Data|Acc]).



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
