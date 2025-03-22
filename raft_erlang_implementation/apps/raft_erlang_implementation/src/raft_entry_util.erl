-module(raft_entry_util).

%% API
-export([sublist_respect_current_direction/3]).
-export([sublist/3]).
-export([find_nth/2]).


sublist_respect_current_direction(_List, StartIndex, EndIndex)
  when StartIndex =:= 0; EndIndex =:= 0 ->
  [];
sublist_respect_current_direction([], _StartIndex, _EndIndex) ->
  [];
sublist_respect_current_direction(List, StartIndex, EndIndex) ->
  lists:sublist(List, StartIndex, EndIndex).


sublist(List, 0, EndIndex) ->
  sublist(List, 1, EndIndex);
sublist(_List, StartIndex, EndIndex)
  when StartIndex < 1 ; EndIndex < 1 ->
  [];
sublist([], _, _) ->
  [];
sublist(List, StartIndex, EndIndex) ->
  ReversedList = lists:reverse(List),
  ReversedSubList = lists:sublist(ReversedList, StartIndex, EndIndex),
  lists:reverse(ReversedSubList).


find_nth(_, []) ->
  undefined;
find_nth(Index, _Entries) when Index < 1 ->
  undefined;
find_nth(Index, Entries) ->
  ReversedEntries = lists:reverse(Entries),
  lists:nth(Index, ReversedEntries).