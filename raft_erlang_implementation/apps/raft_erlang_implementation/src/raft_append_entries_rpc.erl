-module(raft_append_entries_rpc).

%% API
-export([get/2]).
-export([new_append_entries_rpc/6]).

%%  term: leader’s term
%%  leaderId: so follower can redirect clients
%%  prevLogIndex: index of log entry immediately preceding new ones
%%  prevLogTerm:  term of prevLogIndex entry
%%  entries[]: log entries to store (empty for heartbeat; may send more than one for efficiency)
%%  leaderCommit leader’s commitIndex
-record(?MODULE, {
  term :: integer(),
  leader_name :: atom(),
  previous_log_index :: integer(),
  previous_log_term :: integer(),
  entries = [] :: list(),
  leader_commit_index :: integer()
}).

new_append_entries_rpc(Term, LeaderName, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit) ->
  #?MODULE{
    term=Term,
    leader_name = LeaderName,
    previous_log_index = PrevLogIndex,
    previous_log_term = PrevLogTerm,
    entries = Entries,
    leader_commit_index = LeaderCommit}.

get(term, #?MODULE{term=Term}) ->
  Term;
get(leader_name, #?MODULE{leader_name=LeaderName}) ->
  LeaderName;
get(previous_log_index, #?MODULE{previous_log_index=PreviousLogIndex}) ->
  PreviousLogIndex;
get(previous_log_term, #?MODULE{previous_log_term=PreviousLogTerm}) ->
  PreviousLogTerm;
get(entries, #?MODULE{entries=Entries}) ->
  Entries;
get(leader_commit_index, #?MODULE{leader_commit_index=LeaderCommitIndex}) ->
  LeaderCommitIndex;
get(_, _AppendEntries) ->
  undefined.


