-module(raft_rpc_append_entries_test).

%% API
-export([loop_/1]).
-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").


should_append_entries1_test() ->
  %%% GIVEN
  PrevLogIndex = 0,
  PrevLogTerm = 0,
  LogsFromMe = [],

  %%% WHEN
  Result = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, LogsFromMe),

  %%% THEN
  ?assertEqual(true, Result).

should_append_entries2_test() ->
  %%% GIVEN
  PrevLogIndex = 2,
  PrevLogTerm = 2,
  LogsFromMe = [{3, "Hello3"}, {2, "Hello2"}, {1, "Hello1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, LogsFromMe),

  %%% THEN
  ?assertEqual(true, Result).

should_append_entries3_test() ->
  %%% GIVEN
  PrevLogIndex = 2,
  PrevLogTerm = 3,
  LogsFromMe = [{3, "Hello3"}, {2, "Hello2"}, {1, "Hello1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, LogsFromMe),

  %%% THEN
  ?assertEqual(false, Result).

should_append_entries4_test() ->
  %%% GIVEN
  PrevLogIndex = 4,
  PrevLogTerm = 4,
  LogsFromMe = [{3, "Hello3"}, {2, "Hello2"}, {1, "Hello1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, LogsFromMe),

  %%% THEN
  ?assertEqual(false, Result).

should_append_entries5_test() ->
  %%% GIVEN
  PrevLogIndex = 0,
  PrevLogTerm = 0,
  LogsFromMe = [{3, "Hello3"}, {2, "Hello2"}, {1, "Hello1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, LogsFromMe),

  %%% THEN
  ?assertEqual(true, Result).


concat_entries1_test() ->
  %% GIVEN
  LogsIHave = [],
  LogsFromLeader = [],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([], UpdatedLogs),
  ?assertEqual(0, MatchIndex).

concat_entries2_test() ->
  %% GIVEN
  LogsIHave = [],
  LogsFromLeader = [{1, "Hello"}, {2, "Hello"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello"}, {2, "Hello"}], UpdatedLogs),
  ?assertEqual(2, MatchIndex).

concat_entries3_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}],
  LogsFromLeader = [{1, "Hello1"}, {2, "Hello2"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {2, "Hello2"}], UpdatedLogs),
  ?assertEqual(2, MatchIndex).


concat_entries5_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello"}, {1, "Hello"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(2, MatchIndex).

concat_entries6_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(2, MatchIndex).

concat_entries7_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(3, MatchIndex).

concat_entries8_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 2,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}, {1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).

concat_entries9_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 3,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).

concat_entries10_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [{1, "Hello1"}, {1, "Hello2"}],
  PrevIndexFromLeader = 4,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}, {1, "Hello1"}, {1, "Hello2"}], UpdatedLogs),
  ?assertEqual(6, MatchIndex).

concat_entries11_test() ->
  %% GIVEN
  LogsIHave = [{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}],
  LogsFromLeader = [],
  PrevIndexFromLeader = 4,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "Hello1"}, {1, "Hello2"}, {1, "Hello3"}, {1, "Hello4"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).


concat_entries12_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}, {1, "B2"}],
  PrevIndexFromLeader = 2,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "B1"}, {1, "B2"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).

concat_entries13_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "B1"}], UpdatedLogs),
  ?assertEqual(1, MatchIndex).

concat_entries14_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "B1"}], UpdatedLogs),
  ?assertEqual(2, MatchIndex).

concat_entries15_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}],
  PrevIndexFromLeader = 2,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "B1"}], UpdatedLogs),
  ?assertEqual(3, MatchIndex).

concat_entries16_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}],
  PrevIndexFromLeader = 3,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "B1"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).

concat_entries17_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}],
  LogsFromLeader = [{1, "B1"}],
  PrevIndexFromLeader = 4,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "B1"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).

concat_entries18_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).

concat_entries19_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).

concat_entries20_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 2,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(6, MatchIndex).

concat_entries21_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 3,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(7, MatchIndex).

concat_entries22_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 4,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(8, MatchIndex).

concat_entries23_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 5,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(9, MatchIndex).


concat_entries24_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 0,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(4, MatchIndex).


concat_entries25_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}],
  LogsFromLeader = [{1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {1, "B1"}, {2, "B2"}, {3, "B3"}, {4, "B4"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).


concat_entries26_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}],
  LogsFromLeader = [{3, "A2"}, {5, "A3"}, {7, "A6"}, {9, "A7"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A6"}, {9, "A7"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).


concat_entries27_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}],
  LogsFromLeader = [{3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}, {3, "A2"}, {5, "A3"}, {7, "A4"}, {9, "A5"}], UpdatedLogs),
  ?assertEqual(5, MatchIndex).


concat_entries28_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}],
  LogsFromLeader = [],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}], UpdatedLogs),
  ?assertEqual(1, MatchIndex).

concat_entries29_test() ->
  %% GIVEN
  LogsIHave = [{1, "A1"}, {1, "A2"}, {1, "A3"}, {1, "A4"}, {1, "A5"}, {1, "A6"}, {1, "A7"}, {1, "A8"}, {1, "A9"}, {1, "A10"}],
  LogsFromLeader = [],
  PrevIndexFromLeader = 1,

  %% WHEN
  {UpdatedLogs, MatchIndex} = raft_rpc_append_entries:concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader),

  %% THEN
  ?assertEqual([{1, "A1"}], UpdatedLogs),
  ?assertEqual(1, MatchIndex).


do_append_entries1_test() ->
  % If RpcDueTime has been expired and there are no logs to send to, it send append_entry rpc to each member.
  % SETUP
  PidA = start_dummy('A'),
  PidB = start_dummy('B'),
  PidC = start_dummy('C'),

  % GIVEN
  RpcDueTime = raft_rpc_timer_utils:current_time() ,

  MembersExceptMe = ['A', 'B', 'C'],
  MatchIndex = #{'A' => 0, 'B' => 0, 'C' => 0},
  LogEntries = [],
  NextIndex = #{'A' => 1, 'B' => 1, 'C' => 1},
  CurrentTerm = 1,
  CommitIndex = 0,
  RpcDueAcc0 = #{'A' => RpcDueTime, 'B' => RpcDueTime, 'C' => RpcDueTime},

  % WHEN
  timer:sleep(100),
  ResultRpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0),

  % THEN
  Timeout = 500,
  MsgsFromA = get_messages(PidA, Timeout),
  MsgsFromB = get_messages(PidB, Timeout),
  MsgsFromC = get_messages(PidC, Timeout),

  ExpectedAppendEntryMsg = {append_entries, #append_entries{term=1,
                                                            leader_name=undefined,
                                                            previous_log_index=0,
                                                            previous_log_term=0,
                                                            entries=[],
                                                            leader_commit_index=0}},


  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromA),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromB),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromC),
  ?assert(maps:get('A', ResultRpcDue) > RpcDueTime),
  ?assert(maps:get('B', ResultRpcDue) > RpcDueTime),
  ?assert(maps:get('C', ResultRpcDue) > RpcDueTime).

do_append_entries2_test() ->
  % If RpcDueTime has been not expired and there are no logs to send to, it not send append_entry rpc to member.
  % SETUP
  PidA = start_dummy('A'),
  PidB = start_dummy('B'),
  PidC = start_dummy('C'),

  % GIVEN
  RpcDueTime = raft_rpc_timer_utils:current_time() * 2 ,

  MembersExceptMe = ['A', 'B', 'C'],
  MatchIndex = #{'A' => 0, 'B' => 0, 'C' => 0},
  LogEntries = [],
  NextIndex = #{'A' => 1, 'B' => 1, 'C' => 1},
  CurrentTerm = 1,
  CommitIndex = 0,
  RpcDueAcc0 = #{'A' => RpcDueTime, 'B' => RpcDueTime, 'C' => RpcDueTime},

  % WHEN
  ResultRpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0),

  % THEN
  Timeout = 500,
  MsgsFromA = get_messages(PidA, Timeout),
  MsgsFromB = get_messages(PidB, Timeout),
  MsgsFromC = get_messages(PidC, Timeout),

  ?assertEqual([], MsgsFromA),
  ?assertEqual([], MsgsFromB),
  ?assertEqual([], MsgsFromC),

  ?assertEqual(maps:get('A', ResultRpcDue), RpcDueTime),
  ?assertEqual(maps:get('B', ResultRpcDue), RpcDueTime),
  ?assertEqual(maps:get('C', ResultRpcDue), RpcDueTime).


do_append_entries3_test() ->
  % If RpcDueTime has been not expired and there are logs to send to, it send append_entry rpc to member.

  %%% SETUP
  PidA = start_dummy('A'),
  PidB = start_dummy('B'),
  PidC = start_dummy('C'),

  %%% GIVEN
  CurrentTime = raft_rpc_timer_utils:current_time() * 2,

  MembersExceptMe = ['A', 'B', 'C'],
  MatchIndex = #{'A' => 0, 'B' => 0, 'C' => 0},
  LogEntries = [{1, "A2"}, {1, "A1"}],
  NextIndex = #{'A' => 1, 'B' => 1, 'C' => 1},
  CurrentTerm = 1,
  CommitIndex = 0,
  RpcDueAcc0 = #{'A' => CurrentTime, 'B' => CurrentTime, 'C' => CurrentTime},

  % WHEN
  timer:sleep(100),
  ResultRpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0),

  % THEN
  timer:sleep(100),
  Timeout = 500,
  MsgsFromA = get_messages(PidA, Timeout),
  MsgsFromB = get_messages(PidB, Timeout),
  MsgsFromC = get_messages(PidC, Timeout),
  ExpectedAppendEntryMsg = {append_entries, #append_entries{term=1,
                                                            leader_name=undefined,
                                                            previous_log_index=0,
                                                            previous_log_term=0,
                                                            entries=[{1, "A2"}, {1, "A1"}],
                                                            leader_commit_index=0}},

  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromA),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromB),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromC),

  ?assert(maps:get('A', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('B', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('C', ResultRpcDue) =/= CurrentTime).


do_append_entries4_test() ->
  % If RpcDueTime has been expired and there are logs to send to, it send append_entry rpc to member.

  %%% SETUP
  PidA = start_dummy('A'),
  PidB = start_dummy('B'),
  PidC = start_dummy('C'),

  %%% GIVEN
  CurrentTime = raft_rpc_timer_utils:current_time(),

  MembersExceptMe = ['A', 'B', 'C'],
  MatchIndex = #{'A' => 0, 'B' => 0, 'C' => 0},
  LogEntries = [{1, "A2"}, {1, "A1"}],
  NextIndex = #{'A' => 1, 'B' => 1, 'C' => 1},
  CurrentTerm = 1,
  CommitIndex = 0,
  RpcDueAcc0 = #{'A' => CurrentTime, 'B' => CurrentTime, 'C' => CurrentTime},

  % WHEN
  timer:sleep(100),
  ResultRpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0),

  % THEN
  timer:sleep(100),
  Timeout = 500,
  MsgsFromA = get_messages(PidA, Timeout),
  MsgsFromB = get_messages(PidB, Timeout),
  MsgsFromC = get_messages(PidC, Timeout),
  ExpectedAppendEntryMsg = {append_entries, #append_entries{term=1,
                                                            leader_name=undefined,
                                                            previous_log_index=0,
                                                            previous_log_term=0,
                                                            entries=[{1, "A2"}, {1, "A1"}],
                                                            leader_commit_index=0}},

  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromA),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromB),
  ?assertEqual([ExpectedAppendEntryMsg], MsgsFromC),

  ?assert(maps:get('A', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('B', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('C', ResultRpcDue) =/= CurrentTime).

do_append_entries5_test() ->
  % If RpcDueTime has been expired and there are logs to send to, it send append_entry rpc to member.

  %%% SETUP
  PidA = start_dummy('A'),
  PidB = start_dummy('B'),
  PidC = start_dummy('C'),
  PidD = start_dummy('D'),

  %%% GIVEN
  CurrentTime = raft_rpc_timer_utils:current_time(),
  NotExpiredTime = CurrentTime * 2,

  MembersExceptMe = ['A', 'B', 'C', 'D'],
  MatchIndex = #{'A' => 0, 'B' => 1, 'C' => 2, 'D' => 2},
  LogEntries = [{1, "A2"}, {1, "A1"}],
  NextIndex = #{'A' => 1, 'B' => 2, 'C' => 3, 'D' => 3},
  CurrentTerm = 1,
  CommitIndex = 0,
  RpcDueAcc0 = #{'A' => CurrentTime, 'B' => CurrentTime, 'C' => NotExpiredTime, 'D' => CurrentTime},

  % WHEN
  timer:sleep(100),
  ResultRpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0),

  % THEN
  timer:sleep(100),
  Timeout = 500,
  MsgsFromA = get_messages(PidA, Timeout),
  MsgsFromB = get_messages(PidB, Timeout),
  MsgsFromC = get_messages(PidC, Timeout),
  MsgsFromD = get_messages(PidD, Timeout),
  ExpectedAppendEntryMsgA = {append_entries, #append_entries{term=1,
                                                            leader_name=undefined,
                                                            previous_log_index=0,
                                                            previous_log_term=0,
                                                            entries=[{1, "A2"}, {1, "A1"}],
                                                            leader_commit_index=0}},
  ExpectedAppendEntryMsgB = {append_entries, #append_entries{term=1,
                                                             leader_name=undefined,
                                                             previous_log_index=1,
                                                             previous_log_term=1,
                                                             entries=[{1, "A2"}],
                                                             leader_commit_index=0}},
  ExpectedAppendEntryMsgC = [],
  ExpectedAppendEntryMsgD = {append_entries, #append_entries{term=1,
                                                             leader_name=undefined,
                                                             previous_log_index=2,
                                                             previous_log_term=1,
                                                             entries=[],
                                                             leader_commit_index=0}},

  ?assertEqual([ExpectedAppendEntryMsgA], MsgsFromA),
  ?assertEqual([ExpectedAppendEntryMsgB], MsgsFromB),
  ?assertEqual(ExpectedAppendEntryMsgC, MsgsFromC),
  ?assertEqual([ExpectedAppendEntryMsgD], MsgsFromD),

  ?assert(maps:get('A', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('B', ResultRpcDue) =/= CurrentTime),
  ?assert(maps:get('C', ResultRpcDue) =/= CurrentTime).


commit_if_can1_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 1, 'B' => 2, 'C' => 3, 'D' => 4},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 10,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(false, IsSameWithCurrentTerm),
  ?assertEqual(PreviousCommitIndex, MaybeNewCommitIndex).

commit_if_can2_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 3, 'B' => 3, 'C' => 3, 'D' => 3},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 10,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(false, IsSameWithCurrentTerm),
  ?assertEqual(PreviousCommitIndex, MaybeNewCommitIndex).

commit_if_can3_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 4, 'B' => 4, 'C' => 4, 'D' => 4},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 10,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(true, IsSameWithCurrentTerm),
  ?assertEqual(4, MaybeNewCommitIndex).

commit_if_can4_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 3, 'B' => 4, 'C' => 4, 'D' => 4},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 10,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(true, IsSameWithCurrentTerm),
  ?assertEqual(4, MaybeNewCommitIndex).

commit_if_can5_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 3, 'B' => 3, 'C' => 4, 'D' => 4},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 10,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(false, IsSameWithCurrentTerm),
  ?assertEqual(0, MaybeNewCommitIndex).

commit_if_can6_test() ->
  %%% GIVEN
  MatchIndex = #{'A' => 4, 'B' => 4, 'C' => 4, 'D' => 4},
  MemberSize = 4,
  PreviousCommitIndex = 0,
  LogEntries = [{10, "A4"}, {5, "A3"}, {4, "A2"}, {1, "A1"}],
  LeaderTerm = 11,

  %%% WHEN
  {IsSameWithCurrentTerm, MaybeNewCommitIndex} = raft_rpc_append_entries:commit_if_can(MatchIndex, MemberSize, PreviousCommitIndex, LogEntries, LeaderTerm),

  %%% THEN
  ?assertEqual(false, IsSameWithCurrentTerm),
  ?assertEqual(0, MaybeNewCommitIndex).


find_earliest_index_with_same_term1_test() ->
  %%% GIVEN
  PrevTermFromLeader = 5,
  PrevIndex = 6,
  LogEntries = [],

  %%% WHEN
  Result = raft_rpc_append_entries:find_earliest_index_with_same_term(PrevTermFromLeader, PrevIndex, LogEntries),

  %%% THEN
  ?assertEqual(5, Result).

find_earliest_index_with_same_term2_test() ->
  %%% GIVEN
  PrevTermFromLeader = 5,
  PrevIndex = 6,
  LogEntries = [{5, "A6"}, {5, "A5"}, {5, "A4"}, {5, "A3"}, {5, "A2"}, {1, "A1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:find_earliest_index_with_same_term(PrevTermFromLeader, PrevIndex, LogEntries),

  %%% THEN
  ?assertEqual(2, Result).

find_earliest_index_with_same_term3_test() ->
  %%% GIVEN
  PrevTermFromLeader = 5,
  PrevIndex = 6,
  LogEntries = [{5, "A6"}, {5, "A5"}, {5, "A4"}, {5, "A3"}, {5, "A2"}, {5, "A1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:find_earliest_index_with_same_term(PrevTermFromLeader, PrevIndex, LogEntries),

  %%% THEN
  ?assertEqual(0, Result).

find_earliest_index_with_same_term4_test() ->
  %%% GIVEN
  PrevTermFromLeader = 5,
  PrevIndex = 6,
  LogEntries = [{5, "A6"}, {5, "A5"}, {5, "A4"}, {3, "A3"}, {2, "A2"}, {1, "A1"}],

  %%% WHEN
  Result = raft_rpc_append_entries:find_earliest_index_with_same_term(PrevTermFromLeader, PrevIndex, LogEntries),

  %%% THEN
  ?assertEqual(4, Result).


find_earliest_index_at_conflict1_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 0,
  LogEntriesFromMe = [],

  %%% WHEN + THEN
  ExpectedError = {invalid_state, "It cannot be reached at here. because the function `should_append_entries` already filtered it."},
  ?assertError(ExpectedError, raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe)).

find_earliest_index_at_conflict2_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 0,
  LogEntriesFromMe = [],

  %%% WHEN + THEN
  ExpectedError = {invalid_state, "It cannot be reached at here. because the function `should_append_entries` already filtered it."},
  ?assertError(ExpectedError, raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe)).

find_earliest_index_at_conflict3_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 0,
  LogEntriesFromMe = [{1, "A1"}, {2, "A2"}],

  %%% WHEN + THEN
  ExpectedError = {invalid_state, "It cannot be reached at here. because the function `should_append_entries` already filtered it."},
  ?assertError(ExpectedError, raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe)).


find_earliest_index_at_conflict4_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 2,
  LogEntriesFromMe = [{3, "A2"}, {1, "A1"}],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(3, ConflictTerm),
  ?assertEqual(2, FoundFirstIndexWithConflictTerm).

find_earliest_index_at_conflict5_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 3,
  LogEntriesFromMe = [{3, "A2"}, {1, "A1"}],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(3, ConflictTerm),
  ?assertEqual(2, FoundFirstIndexWithConflictTerm).

find_earliest_index_at_conflict6_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 10,
  LogEntriesFromMe = [{3, "A2"}, {1, "A1"}],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(3, ConflictTerm),
  ?assertEqual(2, FoundFirstIndexWithConflictTerm).


find_earliest_index_at_conflict7_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 10,
  LogEntriesFromMe = [],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(0, ConflictTerm),
  ?assertEqual(0, FoundFirstIndexWithConflictTerm).

find_earliest_index_at_conflict8_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 10,
  LogEntriesFromMe = [{3, "A11"}, {3, "A10"}, {3, "A9"}, {3, "A8"},
                      {3, "A7"}, {2, "A6"}, {2, "A5"}, {2, "A4"},
                      {1, "A3"}, {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(3, ConflictTerm),
  ?assertEqual(7, FoundFirstIndexWithConflictTerm).

find_earliest_index_at_conflict9_test() ->
  %%% GIVEN
  PrevIndexFromLeader = 6,
  LogEntriesFromMe = [{3, "A11"}, {3, "A10"}, {3, "A9"}, {3, "A8"},
    {3, "A7"}, {2, "A6"}, {2, "A5"}, {2, "A4"},
    {1, "A3"}, {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevIndexFromLeader, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(2, ConflictTerm),
  ?assertEqual(4, FoundFirstIndexWithConflictTerm).

find_last_index_with_same_term1_test() ->
  %%% GIVEN
  ConflictTerm = 3,
  LogEntriesFromMe = [{3, "A11"}, {3, "A10"}, {3, "A9"}, {3, "A8"},
                      {3, "A7"}, {2, "A6"}, {2, "A5"}, {2, "A4"},
                      {1, "A3"}, {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {IsFoundConflictTermIndex, FoundIndex} = raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(true, IsFoundConflictTermIndex),
  ?assertEqual(11, FoundIndex).

find_last_index_with_same_term2_test() ->
  %%% GIVEN
  ConflictTerm = 3,
  LogEntriesFromMe = [{6, "A10"}, {6, "A9"}, {6, "A8"}, {5, "A7"},
                      {5, "A6"}, {4, "A5"}, {4, "A4"}, {1, "A3"},
                      {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {IsFoundConflictTermIndex, FoundIndex} = raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(false, IsFoundConflictTermIndex),
  ?assertEqual(-1, FoundIndex).


find_last_index_with_same_term3_test() ->
  %%% GIVEN
  ConflictTerm = 2,
  LogEntriesFromMe = [{6, "A10"}, {6, "A9"}, {6, "A8"}, {5, "A7"},
                      {5, "A6"}, {4, "A5"}, {4, "A4"}, {1, "A3"},
                      {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {IsFoundConflictTermIndex, FoundIndex} = raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(false, IsFoundConflictTermIndex),
  ?assertEqual(-1, FoundIndex).


find_last_index_with_same_term4_test() ->
  %%% GIVEN
  ConflictTerm = 1,
  LogEntriesFromMe = [{6, "A10"}, {6, "A9"}, {6, "A8"}, {5, "A7"},
                      {5, "A6"}, {4, "A5"}, {4, "A4"}, {1, "A3"},
                      {1, "A2"}, {1, "A1"}],

  %%% WHEN
  {IsFoundConflictTermIndex, FoundIndex} = raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(true, IsFoundConflictTermIndex),
  ?assertEqual(3, FoundIndex).

find_last_index_with_same_term5_test() ->
  %%% GIVEN
  ConflictTerm = 10,
  LogEntriesFromMe = [],

  %%% WHEN
  {IsFoundConflictTermIndex, FoundIndex} = raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntriesFromMe),

  %%% THEN
  ?assertEqual(false, IsFoundConflictTermIndex),
  ?assertEqual(-1, FoundIndex).



%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%% TEST UTIL Function %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
loop_(Acc) ->
  receive
    {get, From} -> From ! Acc;
    {'$gen_cast', Msg0} -> loop_([Msg0|Acc]);
    Msg -> loop_([Msg|Acc])
  end.


start_dummy(Name) ->
  Pid = spawn_link(raft_rpc_append_entries_test, loop_, [[]]),
  register(Name, Pid),
  Pid.

get_messages(Pid, Timeout) ->
  Pid ! {get, self()},
  receive
    Msg -> Msg
  after Timeout ->
    undefined
  end.