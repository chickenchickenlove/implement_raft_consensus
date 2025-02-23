-module(raft_util).

%% API
-export([set_timer_time/1, get_timer_time/0, clean_up_timer_time/0]).
-export([get_election_timeout_divided_by/1]).

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

get_election_timeout_divided_by(DividedNumber) ->
  ElectionTimeout = get_timer_time(),
  ElectionTimeout div DividedNumber.

