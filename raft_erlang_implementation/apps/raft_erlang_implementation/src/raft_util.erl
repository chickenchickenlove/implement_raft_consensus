-module(raft_util).

%% API
-export([set_timer_time/1, get_timer_time/0, clean_up_timer_time/0]).
-export([get_node_pid/1]).
-export([my_name/0]).
-export([node_name/1]).
-export([compare/2]).
-export([get_entry/3]).

clean_up_timer_time() ->
  catch persistent_term:erase(election_timeout).

set_timer_time(Timeout) ->
  persistent_term:put(election_timeout, Timeout).

get_timer_time() ->
  try
    persistent_term:get(election_timeout)
  catch _:_ ->
    150
  end.

get_node_pid(NodeName) ->
  whereis(NodeName).

my_name() ->
  node_name(self()).

node_name(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.

compare(A, B) ->
  if
    A > B -> greater;
    A =:= B -> equal;
    A < B -> less
  end.

% Assume that [1,2,3,4,5,6, ....]
get_entry(0, _End, _Entries) ->
  [];

get_entry(_Start, 0, _Entries) ->
  [];

get_entry(_Start, _End, []) ->
  [];

get_entry(Start, End, Entries) ->
  lists:sublist(Entries, Start, End).
