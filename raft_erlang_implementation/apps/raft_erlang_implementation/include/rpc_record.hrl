
-record(members, {new_members = sets:new(),
  old_members = sets:new()}
).

-record(vote_granted, {new_members = sets:new(),
  old_members = sets:new()}
).

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


-record(vote_args, {candidate_term :: integer(),
                    candidate_name :: atom(),
                    candidate_last_log_index :: integer(),
                    candidate_last_log_term :: integer()}).

-record(success_append_entries, {match_index :: integer()}).
-record(fail_append_entries, {conflict_term :: integer(),
                              first_index_with_conflict_term :: integer()}).

-record(ack_append_entries, {node_name :: atom(),
                             node_term :: integer(),
                             success :: integer(),
                             result :: #success_append_entries{} | #fail_append_entries{},
                             match_index :: integer()}).

-record(raft_state, {
  %%% Persistent state on all servers. (Updated on stable storage before responding to RPCs)
  % latest term server has seen (initialized to 0 on first boot, increases monotonically)
  current_term = 0 :: integer(),

  % candidate ID that received vote in current term (or null if none)
  voted_for = undefined :: atom(),

  % log entries; each entry contains command for state machine,
  % and term when entry was received by leader (first index is 1).
  log_entries = [] :: list({integer(), any()}),

  %%% Volatile state on all servers
  % index of highest log entry known to be committed.
  % (initialized to 0, increases monotonically)
  commit_index = 0 :: integer(),

  % index of highest log entry applied to state machine.
  % initialized to 0, increases monotonically.
  last_applied = 0 :: integer(),

  % Custom attribute
  last_log_term = 0 :: integer(),
  last_log_index = 0 :: integer(),

  leader = undefined :: atom(),

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  %%%%%%%%%%%% Volatile state on leaders (Reinitialized after election) %%%%%%%%%%%
  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

  rpc_due = #{} :: #{atom() := integer()},

  % index of highest log entry known to be replicated.
  % for each server, index of highest log entry known to be replicated on server.
  % (initialized to 0, increases monotonically)
  match_index = #{} :: #{atom() := integer()},

  % For each server, index of the next log entry to send to that server
  % index of next log entry to send to peer.
  % (initialized to leader last log index + 1)
  next_index = #{} :: #{atom() := integer()},

  members = #members{new_members=sets:new(), old_members=sets:new()} :: #members{},

  vote_granted = #vote_granted{new_members=sets:new(), old_members=sets:new()} :: #vote_granted{},

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  %%%%%%%%%%%%%%%%%%%%% CUSTOM %%%%%%%%%%%%%%%%%%%%%%
  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  election_timeout_timer = undefined,
  append_entries_timer = undefined,
  data = undefined,
  ignore_peer = []
}).

