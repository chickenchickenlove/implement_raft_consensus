-module(raft_log_compaction).

-include("rpc_record.hrl").

%% API
-export([to_start_index/2]).
-export([from_start_index/2]).
-export([actual_index_in_entries/2]).
-export([make_snapshot_if_needed/6]).

to_start_index(Snapshot, RaftLogCompactionMetadata0) ->
  #raft_snapshot{last_included_index=LastIndex} = Snapshot,
  RaftLogCompactionMetadata0#raft_log_compaction_metadata{current_start_index=LastIndex+1}.

from_start_index(Index, #raft_log_compaction_metadata{current_start_index=0}=RaftLogCompactionMetadata) ->
  #raft_log_compaction_metadata{current_start_index=StartIndex} = RaftLogCompactionMetadata,
  max(Index - StartIndex, 0);
from_start_index(Index, RaftLogCompactionMetadata) ->
  #raft_log_compaction_metadata{current_start_index=StartIndex} = RaftLogCompactionMetadata,
  max(Index - StartIndex + 1, 0).

actual_index_in_entries(DesiredIndex, #raft_log_compaction_metadata{current_start_index=0}=RaftLogCompactionMetadata) ->
  #raft_log_compaction_metadata{current_start_index=StartIndex} = RaftLogCompactionMetadata,
  max(DesiredIndex - StartIndex, 0);
actual_index_in_entries(DesiredIndex, #raft_log_compaction_metadata{current_start_index=StartIndex}=RaftLogCompactionMetadata) ->
  #raft_log_compaction_metadata{current_start_index=StartIndex} = RaftLogCompactionMetadata,
  max(DesiredIndex - StartIndex + 1, 0).

%% TODO : Declare Spec.
%% TODO : Put it into proper module. -> raft_log_compaction:handle(...)?
make_snapshot_if_needed(LocalRaftState, RaftLogCompactionMetadata0, CommitIndex, Entries0, LastAppliedEntriesWithIndex, RaftConfiguration) ->
  #raft_configuration{snapshot_module=SnapshotModule, max_log_size=MaxLogSize} = RaftConfiguration,
  case should_compact(Entries0, MaxLogSize) of
    false ->
      {RaftLogCompactionMetadata0, Entries0};
    true ->
      CommitIndexInEntries = actual_index_in_entries(CommitIndex, RaftLogCompactionMetadata0),

      #raft_entry_with_index{raft_entry=LastEntry, raft_index=LastIncludedIndex} = LastAppliedEntriesWithIndex,
      {LastIncludedTerm, _LastLogData} = LastEntry,
      NewSnapshot = SnapshotModule:create_snapshot(LocalRaftState, LastIncludedIndex, LastIncludedTerm),
      SnapshotModule:upsert_snapshot(NewSnapshot),

      % TODO: 여기가 문제다.
      NewStartIndex = CommitIndex + 1,
      NewStartIndexInEntries = CommitIndexInEntries + 1,
      NewEntries = raft_entry_util:sublist(Entries0, NewStartIndexInEntries, length(Entries0)),

      RaftLogCompactionMetadata = RaftLogCompactionMetadata0#raft_log_compaction_metadata{current_start_index=NewStartIndex},
      {RaftLogCompactionMetadata, NewEntries}
  end.

%% @private
should_compact(Entries, MaxLogSize) ->
  length(Entries) > MaxLogSize.

