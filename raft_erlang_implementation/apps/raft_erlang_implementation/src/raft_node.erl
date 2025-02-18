-module(raft_node).

%% API
-export([]).

-record(?MODULE, {

  %%% Persistent state on all servers. (Updated on stable storage before responding to RPCs)
  % latest term server has seen (initialized to 0 on first boot, increases monotonically)
  current_term = 0 :: integer(),

  % candidate ID that received vote in current term (or null if none)
  voted_for = undefined :: atom(),

  % log entries; each entry contains command for state machine,
  % and term when entry was received by leader (first index is 1).
  log_entries = [] :: list(),

  %%% Volatile state on all servers
  % index of highest log entry known to be committed.
  % (initialized to 0, increases monotonically)
  commit_index = 0 :: integer(),

  % index of highest log entry applied to state machine.
  % initialized to 0, increases monotonically.
  last_applied = 0 :: integer(),

  %%% Volatile state on leaders (Reinitialized after election)
  % For each server, index of the next log entry to send to that server
  % (initialized to leader last log index + 1)
  next_index :: integer(),

  % for each server, index of highest log entry known to be replicated on server.
  % (initialized to 0, increases monotonically)
  match_index = 0 :: integer(),

  raft :: atom(),

  members = sets:new() :: sets:sets()
}).

-record(raft, {
  raft_stat_process :: pid()
}).

-behavior(gen_server).

%  A server remains in follower state as long as it receives valid RPCs from a leader or candidate.
% Follower는 Leader, Candidate로부터 RPC를 특정 시간동안 못 받으면 Leader Election을 개최한다.