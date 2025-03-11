-module(raft_rpc_install_snapshot).

-include("rpc_record.hrl").

-export([handle_ack_install_snapshot/2]).
-export([handle_install_snapshot/2]).
-export([do_install_snapshot/1]).
-export([new_ack_install_snapshot_message/3]).

% {install_snapshot, Term, LeaderName, LastIncludedIndex, LastIncludedTerm, Snapshot, Done}
-type rpc_message() :: {install_snapshot}.

% {ack_install_snapshot, IsSuccess, MatchIndex}
-type rpc_ack_message() :: {ack_install_snapshot, atom(), integer(), boolean(), atom()}.


%%-spec()
new_install_snapshot_message(Term, LeaderName, RaftSnapshot) ->
  {install_snapshot, Term, LeaderName, RaftSnapshot, raft_util:my_name()}.

new_ack_install_snapshot_message(Term, IsSuccess, MatchIndex) ->
  {ack_install_snapshot, raft_util:my_name(), Term, IsSuccess, MatchIndex, raft_util:my_name()}.


do_install_snapshot(RaftState) ->
  #raft_state{raft_log_compaction_metadata=RaftLogCompactionMetadata,
              members=Members, next_index=NextIndexes,
              current_term=CurrentTerm,
              raft_configuration=RaftConfiguration} = RaftState,
  #raft_log_compaction_metadata{current_start_index=StartIndex} = RaftLogCompactionMetadata,
  #raft_configuration{snapshot_module=SnapshotModule} = RaftConfiguration,

  Snapshot = SnapshotModule:get_snapshot(),

  case Snapshot of
    undefined -> ok;
    Snapshot ->
      MembersExceptMe = raft_util:members_except_me(Members),
      MembersNeedToInstallSnapshot = filter_members_(MembersExceptMe, StartIndex, NextIndexes),
      send_install_snapshot_rpc(MembersNeedToInstallSnapshot, Snapshot, CurrentTerm)
  end.

handle_install_snapshot({install_snapshot, TermFromPeer, LeaderName, RaftSnapshot, _FromNode}=Msg, State0) ->
  #raft_state{raft_configuration=RaftConfiguration, raft_log_compaction_metadata=RaftLogCompactionMetadata0,
              current_term=CurrentTerm,
              log_entries=Entries0} = State0,
  #raft_configuration{snapshot_module=SnapshotModule} = RaftConfiguration,

  SnapshotModule:upsert_snapshot(RaftSnapshot),
  {MaybeNewMatchIndex, MaybeNewEntries, MaybeNewRaftLogCompactionMetadata}
    = remove_entries_if_needed(RaftSnapshot, RaftLogCompactionMetadata0, Entries0),

  AckMsg = new_ack_install_snapshot_message(CurrentTerm, true, MaybeNewMatchIndex),
  send_msg_(LeaderName, AckMsg),

  State0#raft_state{log_entries=MaybeNewEntries, raft_log_compaction_metadata=MaybeNewRaftLogCompactionMetadata}.

handle_ack_install_snapshot({ack_install_snapshot, PeerName, PeerTerm, IsSuccess, MatchIndex, _FromNode}, State0) ->
  case IsSuccess of
    %%% Currently, IsSuccess is always `true`.
    false -> State0;
    true ->
      #raft_state{match_index=MatchIndexes0, next_index=NextIndexes0} = State0,
      MatchIndexes = maps:put(PeerName, MatchIndex, MatchIndexes0),
      NextIndexes = maps:put(PeerName, MatchIndex + 1, NextIndexes0),
      State0#raft_state{match_index=MatchIndexes, next_index=NextIndexes}
  end.

send_install_snapshot_rpc(Members, RaftSnapshot, CurrentTerm) ->
  Message = new_install_snapshot_message(CurrentTerm, raft_util:my_name(), RaftSnapshot),
  lists:foreach(fun(PeerName) ->
    send_msg_(PeerName, Message)
  end, Members).


send_msg_(PeerName, Message) ->
  ToMemberPid = raft_util:get_node_pid(PeerName),
  gen_statem:cast(ToMemberPid, Message).

%% @private
remove_entries_if_needed(RaftSnapshot, RaftLogCompactionMetadata0, Entries0) ->
  #raft_snapshot{last_included_index=LastIncludedIndex, last_included_term=LastIncludedTerm} = RaftSnapshot,
  TargetEntryIndexInLogs = raft_log_compaction:actual_index_in_entries(LastIncludedIndex, RaftLogCompactionMetadata0),
  NewEntries =
    case raft_entry_util:find_nth(TargetEntryIndexInLogs, Entries0) of
      undefined -> [];
      EntryInLastIncludedIndex ->
        {Term, _Data} = EntryInLastIncludedIndex,
        case Term =:= LastIncludedTerm of
          true -> raft_entry_util:sublist(Entries0, TargetEntryIndexInLogs, length(Entries0));
          false -> []
        end
    end,

  RaftLogCompactionMetadata = raft_log_compaction:to_start_index(RaftSnapshot, RaftLogCompactionMetadata0),
  {LastIncludedIndex, NewEntries, RaftLogCompactionMetadata}.

%% @private
filter_members_(MembersExceptMe, FirstIndexIHave, NextIndexes) ->
  Filter = fun(PeerName) ->
    NextIndexOfPeer = maps:get(PeerName, NextIndexes),
    should_install_snapshot(FirstIndexIHave, NextIndexOfPeer)
    end,
  lists:filter(Filter, MembersExceptMe).

%% @private
should_install_snapshot(FirstIndexIHave, NextIndexOfPeer) ->
  FirstIndexIHave > NextIndexOfPeer.