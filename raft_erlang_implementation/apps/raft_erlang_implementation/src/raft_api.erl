-module(raft_api).

-include("rpc_record.hrl").

%% API
-export([handle_event/4]).
-export([confirm_new_cluster_membership/2]).
-export([prepare_new_cluster_membership/2]).
-export([add_members/2]).
-export([add_entry_async/3]).
-export([remove_members/2]).
-export([get_new_members/1]).
-export([get_old_members/1]).
-export([get_timer/1]).
-export([get_voted_for/1]).
-export([get_state/1]).
-export([get_voted_count/1]).
-export([get_current_term/1]).
-export([get_log_entries/1]).
-export([get_commit_index/1]).
-export([get_leader_pid/1]).
-export([get_snapshot/1]).

-export([set_ignore_msg_from_this_peer/2]).
-export([unset_ignore_peer/1]).

-spec prepare_new_cluster_membership(NodeNameOrPid, NewMemberShip) -> ok when
  NodeNameOrPid :: pid() | atom(),
  NewMemberShip :: list(pid).
prepare_new_cluster_membership(NodeName, NewMemberShip) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:cast(Pid, {new_entry, {new_membership, NewMemberShip}, self()});
prepare_new_cluster_membership(Pid, NewMemberShip)  ->
  gen_statem:cast(Pid, {new_entry, {new_membership, NewMemberShip}, self()}).

-spec confirm_new_cluster_membership(NodeNameOrPid, NewMemberShip) -> ok when
  NodeNameOrPid :: pid() | atom(),
  NewMemberShip :: list(pid).
confirm_new_cluster_membership(NodeName, NewMemberShip) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:cast(Pid, {new_entry, {confirm_new_membership, NewMemberShip}, self()});
confirm_new_cluster_membership(Pid, NewMemberShip)  ->
  gen_statem:cast(Pid, {new_entry, {confirm_new_membership, NewMemberShip}, self()}).


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

add_entry_async(NodeName, Entry, From) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:cast(Pid, {new_entry, Entry, From});
add_entry_async(Pid, Entry, From) ->
  gen_statem:cast(Pid, {new_entry, Entry, From}).

get_snapshot(Pid) ->
  gen_statem:call(Pid, get_snapshot).

get_new_members(Pid) ->
  gen_statem:call(Pid, get_new_members).

get_old_members(Pid) ->
  gen_statem:call(Pid, get_old_members).

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

get_commit_index(Pid) ->
  gen_statem:call(Pid, get_commit_index).

get_leader_pid(Pid) ->
  gen_statem:call(Pid, get_leader_pid).

% THIS IS ONLY FOR TEST
% IgnoreNodeNames = [atom()]
set_ignore_msg_from_this_peer(Pid, IgnoreNodeNames) ->
  gen_statem:call(Pid, {set_ignore_this_peer, IgnoreNodeNames}).

% THIS IS ONLY FOR TEST
unset_ignore_peer(Pid) ->
  gen_statem:call(Pid, unset_ignore_this_peer).

handle_event({call, From}, get_commit_index, _State, Data) ->
  #raft_state{commit_index=CommitIndex} = Data,
  {keep_state, Data, [{reply, From, CommitIndex}]};

handle_event({call, From}, get_leader_pid, _State, Data) ->
  #raft_state{leader=LeaderName} = Data,
  LeaderPid = raft_util:get_node_pid(LeaderName),
  {keep_state, Data, [{reply, From, LeaderPid}]};

handle_event({call, From}, get_timer, _State, Data) ->
  #raft_state{election_timeout_timer=Timer}=Data,
  {keep_state, Data, [{reply, From, Timer}]};
handle_event({call, From}, get_state, State, Data) ->
  {keep_state, Data, [{reply, From, State}]};
handle_event({call, From}, get_log_entries, _State, Data) ->
  #raft_state{log_entries=LogEntries} = Data,
  {keep_state, Data, [{reply, From, LogEntries}]};

handle_event({call, From}, get_voted_count, _State, Data) ->
  #raft_state{vote_granted=VoteGranted} = Data,
  #vote_granted{new_members=FromNewMembers, old_members=FromOldMembers} = VoteGranted,
  VotedCount = sets:size(FromNewMembers) + sets:size(FromOldMembers),
  {keep_state, Data, [{reply, From, VotedCount}]};

handle_event({call, From}, get_snapshot, _State, Data) ->
  #raft_state{raft_configuration=RaftConfiguration} = Data,
  #raft_configuration{snapshot_module=SnapshotModule} = RaftConfiguration,
  Snapshot = SnapshotModule:get_snapshot(),
  {keep_state, Data, [{reply, From, Snapshot}]};

handle_event({call, From}, get_new_members, _State, Data) ->
  #raft_state{members=Members} = Data,
  #members{new_members=NewMembers} = Members,
  {keep_state, Data, [{reply, From, NewMembers}]};

handle_event({call, From}, get_old_members, _State, Data) ->
  #raft_state{members=Members} = Data,
  #members{old_members=OldMembers} = Members,
  {keep_state, Data, [{reply, From, OldMembers}]};

handle_event({call, From}, get_current_term, _State, Data) ->
  #raft_state{current_term=CurrentTerm} = Data,
  {keep_state, Data, [{reply, From, CurrentTerm}]};

handle_event({call, From}, get_voted_for, _State, Data) ->
  #raft_state{voted_for=VotedFor} = Data,
  {keep_state, Data, [{reply, From, VotedFor}]};

handle_event({call, From}, {set_ignore_this_peer, IgnoreNodeName}, _State, Data0) ->
  Data = Data0#raft_state{ignore_peer=IgnoreNodeName},
  {keep_state, Data, [{reply, From, ok}]};

handle_event({call, From}, unset_ignore_this_peer, _State, Data0) ->
  Data = Data0#raft_state{ignore_peer=[]},
  {keep_state, Data, [{reply, From, ok}]};

handle_event(_EventType, _EventCount, _State, Data) ->
  %% Ignore all other events.
  {keep_state, Data}.