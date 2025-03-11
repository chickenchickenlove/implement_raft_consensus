-module(raft_entry_util_test).

-include("rpc_record.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

sublist1_test() ->
  %%% GIVEN
  List = [5,4,3,2,1],
  StartIndex = 1,
  EndIndex = 3,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([3, 2, 1], Result).

sublist2_test() ->
  %%% GIVEN
  List = [],
  StartIndex = 1,
  EndIndex = 3,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([], Result).


sublist3_test() ->
  %%% GIVEN
  List = [],
  StartIndex = 5,
  EndIndex = 8,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([], Result).

sublist4_test() ->
  %%% GIVEN
  List = [5, 4, 3, 2, 1],
  StartIndex = 5,
  EndIndex = 6,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([5], Result).

sublist5_test() ->
  %%% GIVEN
  List = [5, 4, 3, 2, 1],
  StartIndex = 5,
  EndIndex = 5,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([5], Result).


sublist6_test() ->
  %%% GIVEN
  List = [5, 4, 3, 2, 1],
  StartIndex = 6,
  EndIndex = 7,

  %%% WHEN
  Result = raft_entry_util:sublist(List, StartIndex, EndIndex),

  %%% THEN
  ?assertEqual([], Result).




