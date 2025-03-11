-module(raft_index_util).

-include("rpc_record.hrl").
%% API
-export([to_last_log_entry_with_index/2]).
-export([from_last_log_entry_with_index/1]).


to_last_log_entry_with_index(LastAppliedEntry, StartIndex) ->
  #raft_entry_with_index{raft_entry=LastAppliedEntry, raft_index=StartIndex-1}.


from_last_log_entry_with_index(LastRaftEntryWithIndex) ->
  #raft_entry_with_index{raft_entry=LastAppliedEntry} = LastRaftEntryWithIndex,
  LastAppliedEntry.