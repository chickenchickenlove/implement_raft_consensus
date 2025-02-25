-module(raft_rpc_append_entries).

-include("rpc_record.hrl").

%% API
-export([get/2]).
-export([new/6]).
-export([new_ack/4]).
-export([new_ack_fail/2]).

new(Term, LeaderName, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit) ->
  #append_entries{term=Term,
                  leader_name = LeaderName,
                  previous_log_index = PrevLogIndex,
                  previous_log_term = PrevLogTerm,
                  entries = Entries,
                  leader_commit_index = LeaderCommit}.

new_ack_fail(NodeName, NodeTerm) ->
  % -1 is invalid value.
  % So, Caller should ignore this value if caller witness this.
  new_ack(NodeName, NodeTerm, false, -1).


new_ack(NodeName, NodeTerm, Success, MatchIndex) ->
  #ack_append_entries{node_name=NodeName,
                      node_term=NodeTerm,
                      success=Success,
                      match_index=MatchIndex}.


get(term, #append_entries{term=Term}) ->
  Term;
get(leader_name, #append_entries{leader_name=LeaderName}) ->
  LeaderName;
get(previous_log_index, #append_entries{previous_log_index=PreviousLogIndex}) ->
  PreviousLogIndex;
get(previous_log_term, #append_entries{previous_log_term=PreviousLogTerm}) ->
  PreviousLogTerm;
get(entries, #append_entries{entries=Entries}) ->
  Entries;
get(leader_commit_index, #append_entries{leader_commit_index=LeaderCommitIndex}) ->
  LeaderCommitIndex;
get(_, _AppendEntries) ->
  undefined.


