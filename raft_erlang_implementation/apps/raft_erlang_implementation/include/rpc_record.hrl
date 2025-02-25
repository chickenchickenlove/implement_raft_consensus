
%%  term: leader’s term
%%  leaderId: so follower can redirect clients
%%  prevLogIndex: index of log entry immediately preceding new ones
%%  prevLogTerm:  term of prevLogIndex entry
%%  entries[]: log entries to store (empty for heartbeat; may send more than one for efficiency)
%%  leaderCommit leader’s commitIndex
-record(append_entries, {term :: integer(),
  leader_name :: atom(),
  previous_log_index :: integer(),
  previous_log_term :: integer(),
  entries = [] :: list(),
  leader_commit_index :: integer()
}).


%%CandidateTerm, CandidateName, _CandidateLastLogIndex, _CandidateLastLogTerm
-record(vote_args, {candidate_term :: integer(),
                    candidate_name :: atom(),
                    candidate_last_log_index :: integer(),
                    candidate_last_log_term :: integer()}).

-record(ack_append_entries, {node_name :: atom(),
  node_term :: integer(),
  success :: integer(),
  match_index :: integer()}).