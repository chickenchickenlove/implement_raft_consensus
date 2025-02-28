-module(raft_rpc_append_entries_test).

%% API
-export([]).
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