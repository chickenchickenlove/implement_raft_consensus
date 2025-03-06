-module(raft_api).

%% API
-export([confirm_new_cluster_membership/2]).
-export([prepare_new_cluster_membership/2]).
-export([add_members/2]).
-export([remove_members/2]).
-export([add_entry/2]).
-export([get_timer/1]).
-export([get_voted_for/1]).
-export([get_state/1]).
-export([get_voted_count/1]).
-export([get_current_term/1]).
-export([get_log_entries/1]).


-spec prepare_new_cluster_membership(NodeNameOrPid, NewMemberShip) -> ok when
  NodeNameOrPid :: pid() | atom(),
  NewMemberShip :: list(pid).
prepare_new_cluster_membership(NodeName, NewMemberShip) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {new_entry, {new_membership, NewMemberShip}});
prepare_new_cluster_membership(Pid, NewMemberShip)  ->
  gen_statem:call(Pid, {new_entry, {new_membership, NewMemberShip}}).

-spec confirm_new_cluster_membership(NodeNameOrPid, NewMemberShip) -> ok when
  NodeNameOrPid :: pid() | atom(),
  NewMemberShip :: list(pid).
confirm_new_cluster_membership(NodeName, NewMemberShip) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {new_entry, {confirm_new_membership, NewMemberShip}});
confirm_new_cluster_membership(Pid, NewMemberShip)  ->
  gen_statem:call(Pid, {new_entry, {confirm_new_membership, NewMemberShip}}).


-spec add_members(NodeNameOrPid, NewMembers) -> ok when
  NodeNameOrPid :: pid() | atom(),
  NewMembers :: list(pid).
add_members(NodeName, NewMembers) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {new_entry, {add_members, NewMembers}});
add_members(Pid, NewMembers)  ->
  gen_statem:call(Pid, {new_entry, {add_members, NewMembers}}).

-spec remove_members(NodeNameOrPid, RemovedMembers) -> ok when
  NodeNameOrPid :: pid() | atom(),
  RemovedMembers :: list(pid).
remove_members(NodeName, RemovedMembers) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {new_entry, {remove_members, RemovedMembers}});
remove_members(Pid, RemovedMembers)  ->
  gen_statem:call(Pid, {new_entry, {remove_members, RemovedMembers}}).

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
