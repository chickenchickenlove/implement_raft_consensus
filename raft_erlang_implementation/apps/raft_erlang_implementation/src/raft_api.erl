-module(raft_api).

%% API
-export([add_entry/2]).
-export([get_timer/1]).
-export([get_voted_for/1]).
-export([get_state/1]).
-export([get_voted_count/1]).
-export([get_current_term/1]).
-export([get_log_entries/1]).


add_entry(NodeName, Entry) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {new_entry, Entry});
add_entry(Pid, Entry) ->
  gen_statem:call(Pid, {new_entry, Entry}).

get_timer(Pid) ->
  gen_statem:call(Pid, get_timer).

get_voted_for(Pid) ->
  gen_statem:call(Pid, get_voted_for).

get_state(Pid) ->
  gen_statem:call(Pid, get_state).

get_voted_count(Pid) ->
  gen_statem:call(Pid, get_voted_count).

get_current_term(Pid) ->
  gen_statem:call(Pid, get_current_term).

get_log_entries(Pid) ->
  gen_statem:call(Pid, get_log_entries).
