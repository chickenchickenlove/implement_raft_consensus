-module(raft_node_state_machine).

-behavior(gen_statem).

%%% Reference1 : https://raft.github.io/raft.pdf
%%% Reference2 : https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
%%% pseudo code : https://github.com/ongardie/raft-pseudocode

-include("rpc_record.hrl").

%% API
-export([init/1]).
-export([start/2]).
-export([stop/1]).
-export([callback_mode/0]).

%% State Function
-export([follower/3]).
-export([candidate/3]).
-export([leader/3]).
-export([deprecated/3]).

-type raft_state() :: follower   |
                      leader     |
                      candidate  |
                      % THIS IS NOT OFFICIAL RAFT STATE. IT IS ONLY FOR DISPLAYING DEPRECATED OLD NODES AS RESULT OF CLUSTER MEMBERSHIP CHANGE.
                      deprecated.

%%%%%%%%%%%%%%%%%% NOTE %%%%%%%%%%%%%%%%%%%%%
% Follower remains in follower state as long as it receives valid RPCs from a leader or candidate.

%% Use State Function. Not a event handler.
callback_mode() ->
  state_functions.

%% API.
start(NodeName, Members) ->
  MemberSet = sets:from_list(Members),
  gen_statem:start(?MODULE, {NodeName, MemberSet}, []).

-define(ELECTION_TIMEOUT, 10000).

%%% Mandatory callback functions.
init({NodeName, NewMembers}) ->

  erlang:register(NodeName, self()),
  % All node will be started with `follower` state.
  State = follower,

  Members = #members{new_members=NewMembers, old_members=sets:new()},

  MatchIndex = init_match_index(Members),
  NextIndex = init_next_index(Members),
  RpcDue = init_rpc_due(Members),

  Timer = raft_scheduler:schedule_heartbeat_timeout(),
  Data = #raft_state{election_timeout_timer=Timer,
                     members=Members,
                     match_index=MatchIndex,
                     next_index=NextIndex,
                     rpc_due=RpcDue},
  {ok, State, Data}.

stop(NodeName) ->
  Pid = raft_util:get_node_pid(NodeName),
  gen_statem:stop(Pid),
  timer:sleep(50).

% 5.1 Raft Basics
% The leader handles all client requests (if a client contacts a follower,
% the follower redirects it to the leader).

% Append Entries Ack Implement.
%% Results:
%%  term: currentTerm, for leader to update itself
%%  success: true if follower contained entry matching  prevLogIndex and prevLogTerm

%% Receiver implementation:
%% 1. Reply false if term < currentTerm (§5.1)
%% 2. Reply false if log doesn’t contain an entry at prevLogIndex
%%    whose term matches prevLogTerm (§5.3)
%% 3. If an existing entry conflicts with a new one (same index but different terms),
%%    delete the existing entry and all that follow it (§5.3)
%% 4. Append any new entries not already in the log
%% 5. If leaderCommit > commitIndex, set commitIndex


%%% Follower should reset its election timeout when it receive valid RPC.
%%%   1) AppendEntries RPC from valid leader.
%%%   2) Request Voted RPC from valid candidate.
%%%   3) InstallSnapshot RPC from valid leader.
follower(cast, deprecated, Data0) ->
  {next_state, deprecated, Data0};

follower(cast, {new_entry, Entry, FromClient}, Data0) ->
  #raft_state{leader=LeaderName} = Data0,
  LeaderPid = raft_util:get_node_pid(LeaderName),
  raft_api:add_entry_async(LeaderPid, Entry, my_name()),
  {keep_state, Data0};

follower(cast, {append_entries, AppendEntriesRpc, FromNodeName},
         #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {append_entries, AppendEntriesRpc, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm}=AppendEntriesRpc, _FromNodeName},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries from ~p, but its Term is bigger than me, so this node turns to follwer.~n", [self(), my_name(), AppendEntriesTerm]),
  NextEvent = next_event(cast, {append_entries, AppendEntriesRpc}),
  step_down(AppendEntriesTerm, Data0, NextEvent, follower);

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc, _FromNodeName},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got invalid append_entries from ~p. ~n", [self(), my_name(), LeaderName]),

  ToPid = raft_util:get_node_pid(LeaderName),
  AckAppendEntries = raft_rpc_append_entries:new_ack_fail_with_default(my_name(), CurrenTerm),
  AckMsg = raft_rpc_append_entries:new_ack_append_entries_msg(AckAppendEntries),

  gen_statem:cast(ToPid, AckMsg),
  {keep_state, Data0};

follower(cast, {append_entries, AppendEntriesRpc, _FromNodeName}, Data0)  ->
  Data1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  #raft_state{log_entries=Logs, current_term=CurrentTerm, commit_index=CommitIndex0,
              last_log_term=LastLogTerm0, last_log_index=LastLogIndex0,
              last_applied=LastAppliedEntries0, raft_log_compaction_config=RaftLogCompactionConfig0,
              raft_configuration=RaftConfiguration,
              local_raft_state=LocalRaftState0, first_index_in_current_entries=FirstIndexInCurrentEntries0,
              data=AppliedData0} = Data1,
  #append_entries{leader_name=LeaderName,
                  entries=LogsFromLeader,
                  leader_commit_index=LeaderCommitIndex,
                  previous_log_index=PrevLogIndex,
                  previous_log_term=PrevLogTerm} = AppendEntriesRpc,

  AppliedData = safe_get_entry_at_index(Logs, LeaderCommitIndex, AppliedData0),
  io:format("[~p] Node ~p got valid append_entries rpc from ~p.~n", [self(), my_name(), LeaderName]),

  NewLeader = LeaderName,
  IsSuccess = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, Logs),
  {UpdatedLogEntries, CommitIndex, AckAppendEntries, LastLogTerm, LastLogIndex}
    = case IsSuccess of
        true ->
          %% Since the leader has declared that the index up to leaderCommit is “already safely replicated”, we can assume that the follower can commit up to that index.
          %% However, it is possible that the follower has not physically received logs up to that index yet, meaning that the actual length of the follower's logs (MatchIndex0) may be less than leaderCommit.
          %% Therefore, the follower can't commit to an index it doesn't actually have, so it must eventually commit up to the value of min(leaderCommit, the actual last index).
          {UpdatedLogEntries0, MatchIndex0} = raft_rpc_append_entries:do_concat_entries(Logs, LogsFromLeader, PrevLogIndex),
          CommitIndex1 = min(LeaderCommitIndex, MatchIndex0),
          {MaybeNewLastLogTerm, MaybeNewLastLogIndex} =
            raft_rpc_append_entries:get_last_log_term_and_index(UpdatedLogEntries0, LastLogTerm0, LastLogIndex0),
          SuccessAppendEntries = raft_rpc_append_entries:new_ack_success(my_name(), CurrentTerm, MatchIndex0),
          {UpdatedLogEntries0, CommitIndex1, SuccessAppendEntries, MaybeNewLastLogTerm, MaybeNewLastLogIndex};
        false ->
          {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevLogIndex, Logs),
          FailAppendEntries = raft_rpc_append_entries:new_ack_fail(my_name(), CurrentTerm, ConflictTerm, FoundFirstIndexWithConflictTerm),
          {Logs, CommitIndex0, FailAppendEntries, LastLogTerm0, LastLogIndex0}
          end,

  ToPid = raft_util:get_node_pid(LeaderName),

  AckMsg = raft_rpc_append_entries:new_ack_append_entries_msg(AckAppendEntries),
  gen_statem:cast(ToPid, AckMsg),
  Data2 = Data1#raft_state{leader=NewLeader, commit_index=CommitIndex,
                           last_log_term=LastLogTerm, last_log_index=LastLogIndex,
                           log_entries=UpdatedLogEntries, data=AppliedData},

  ShouldHandleEntries = raft_util:get_entry(CommitIndex0, CommitIndex, UpdatedLogEntries),
  Data3 = raft_cluster_change:handle_cluster_change_if_needed(ShouldHandleEntries, false, Data2),

  {MaybeNewRaftState, MaybeNewLastAppliedEntry} = apply_entries_to_state(UpdatedLogEntries, FirstIndexInCurrentEntries0, CommitIndex0, CommitIndex, LocalRaftState0, LastAppliedEntries0),
  make_snapshot_if_needed(MaybeNewRaftState, RaftLogCompactionConfig0, CommitIndex, UpdatedLogEntries, RaftConfiguration),
  Data4 = Data3#raft_state{}


  {keep_state, Data3};

follower(cast, {ack_append_entries, AckAppendEntries, FromNodeName},
         #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_append_entries, AckAppendEntries, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

follower(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}, _FromNodeName},
    #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  step_down(NodeTerm, Data0, follower);

follower(cast, election_timeout, Data) ->
  io:format("[~p] Node ~p got election_timeout. from now on, it is candidate follower. leader election will be held in a few second. ~n", [self(), my_name()]),
  NextEvent = next_event(cast, start_leader_election),
  {next_state, candidate, Data, NextEvent};

% Each server will vote for at most one candidate in a given term, on a `first-come-first-served basis`.
% 1. If Candidate Term is higher than me, vote.
% 2. If both Candidate term and my term are same, follow this prioirty
%   2.1 Bigger Last Log Term.
%   2.2 If Last Log Term is same, Bigger Last Log Index

%% Receiver implementation:
%%   1. Reply false if term < currentTerm (§5.1)
%%   2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

follower(cast, {request_vote, VoteArgs, FromNodeName},
         #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {request_vote, VoteArgs, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

follower(cast, {request_vote, VoteArgs, FromNodeName}, #raft_state{current_term=CurrentTerm0}=Data0) ->
  #vote_args{candidate_term=CandidateTerm} = VoteArgs,
  io:format("[~p] Node ~p got request_vote from ~p~n", [self(), my_name(), FromNodeName]),

  Data1 =
    case CurrentTerm0 < CandidateTerm of
      true -> Data0#raft_state{current_term=CandidateTerm, voted_for=undefined};
      false -> Data0
    end,

  raft_rpc_request_vote:handle_request_vote_rpc(Data1, VoteArgs);

follower(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted, FromNodeName},
    #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

follower(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted, _FromNodeName},
    #raft_state{current_term=CurrentTerm}=Data0) ->
  io:format("[~p] Node ~p got ack_request_voted ~n", [self(), my_name()]),
  case CurrentTerm < ResponseTerm of
    true -> step_down(ResponseTerm, Data0, follower);
    false -> {keep_state, Data0}
  end;

follower(EventType, EventContent, Data) ->
  raft_api:handle_event(EventType, EventContent, follower, Data).

%% (a) it wins the election,
%% (b) another server establishes itself as leader
%% (c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below

candidate(cast, deprecated, Data0) ->
  {next_state, deprecated, Data0};

candidate(cast, start_leader_election, #raft_state{current_term=CurrentTerm}=Data0) ->
  io:format("[~p] Node ~p start leader election. Term is ~p.~n", [self(), my_name(), CurrentTerm]),
  % Should consider deadlock.
  % So, we DO NOT USE syncronous function.
  Data1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  Data2 = clear_vote_granted(Data1),
  Data3 = raft_rpc_request_vote:vote_my_self(CurrentTerm + 1, Data2),

  #raft_state{members=Members, current_term=NewlyCurrentTerm,
              log_entries=LogEntries} = Data3,

  RpcDue = init_rpc_due(Members),
  NextIndex = init_next_index(Members),
  MatchIndex = init_match_index(Members),
  Data4 = Data3#raft_state{rpc_due=RpcDue, next_index=NextIndex, match_index=MatchIndex},

  LastLogIndex = length(LogEntries),
  LastLogTerm = log_term(LogEntries, LastLogIndex),

  VoteArgs = raft_rpc_request_vote:new_vote_arguments(my_name(), NewlyCurrentTerm, LastLogIndex, LastLogTerm),
  RpcDueExceptMe = maps:filter(fun(Member, _Timeout) ->
                                  Member =/= my_name()
                                end, RpcDue),

  SendRequestVoteIfNodeIsExisted =
    fun(ToMemberName, _Timeout, Acc) ->
      ToMemberPid = raft_util:get_node_pid(ToMemberName),
      try
        raft_rpc_request_vote:request_vote(ToMemberPid, VoteArgs),
        maps:put(ToMemberName, raft_rpc_timer_utils:next_rpc_due(), Acc)
      catch
        exit:{noproc, _}  ->
          io:format("There is no such registred process ~p yet. ~n", [ToMemberPid]),
          Acc
      end
    end,
  NewRpcDue = maps:fold(SendRequestVoteIfNodeIsExisted, #{}, RpcDueExceptMe),
  Data5 = Data4#raft_state{rpc_due=NewRpcDue},
  NextEvent = next_event(cast, can_become_leader),

  {keep_state, Data5, NextEvent};


candidate(cast, {request_vote, VoteArgs, FromNodeName},
          #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {request_vote, VoteArgs, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

candidate(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs, FromName},
          #raft_state{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  io:format("[~p] Node ~p got request_vote from ~p, but its term is bigger than me. so this node turns to follower. ~n", [self(), my_name(), CandidateTerm]),
  NextEvent = next_event(cast, {request_vote, VoteArgs, FromName}),
  step_down(CandidateTerm, Data0, NextEvent, candidate);

candidate(cast, {request_vote, VoteArgs, FromName}, Data0) ->
  io:format("[~p] Node ~p got request_vote from ~p.~n", [self(), my_name(), FromName]),
  %%% Candidate will not vote to other.
  %%% Because, Candidate already has been voted itself.
  raft_rpc_request_vote:handle_request_vote_rpc(Data0, VoteArgs);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, FromNodeName},
    #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true, _FromNodeName},
          #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < ResponseTerm->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  step_down(ResponseTerm, Data0, candidate);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true, _FromNodeName}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  #raft_state{current_term=CurrentTerm}=Data0,
  Data =
    case ResponseTerm =:= CurrentTerm of
      true ->
        #raft_state{vote_granted=VoteGranted0, rpc_due=RpcDue0, members=Members} = Data0,
        RpcDue1 = maps:put(FromName, raft_rpc_timer_utils:infinity_rpc_due(), RpcDue0),
        VoteGranted1 = raft_rpc_request_vote:vote_granted(FromName, Members, VoteGranted0),
        Data0#raft_state{vote_granted=VoteGranted1, rpc_due=RpcDue1};
      false -> Data0
    end,
  NextEvent = next_event(cast, can_become_leader),
  {keep_state, Data, NextEvent};

candidate(cast, {ack_request_voted, FromName, _ResponseTerm, false, _FromNodeName}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  {keep_state, Data0};

candidate(cast, can_become_leader, Data0) ->
  io:format("[~p] Node ~p got can_become_leader msg ~n", [self(), my_name()]),
  #raft_state{vote_granted=VoteGranted, members=Members, current_term=CurrentTerm} = Data0,

  case raft_rpc_request_vote:has_quorum(Members, VoteGranted) of
    true ->
      #raft_state{next_index=NextIndex0, log_entries=Logs} = Data0,
      Length = length(Logs),
      %% 5.3 Log Replication in Reference1.
      %% When a leader first comes to power,
      %% it initializes all nextIndex values to the index just after the last one in its log (11 in Figure 7).
      NextIndex = maps:fold(fun(MemberName, _NextIndex, Acc) ->
                              maps:put(MemberName, Length + 1, Acc)
                            end, #{}, NextIndex0),
      InitRpcDue = init_rpc_due(Members),
      Data1 = Data0#raft_state{leader=my_name(), next_index=NextIndex, rpc_due=InitRpcDue},
      NextEvent = next_event(cast, do_append_entries),
      io:format("[~p] node ~p win the leader election. term is ~p.~n", [self(), my_name(), CurrentTerm]),
      {next_state, leader, Data1, NextEvent};
    false ->
      io:format("[~p] node ~p failed to win the leader election. term is ~p.~n", [self(), my_name(), CurrentTerm]),
      {keep_state, Data0}
  end;

% In some situations an election will result in a split vote. In this case the term
% will end with no leader; a new term (with a new election) will begin shortly.
% Raft ensures that there is at most one leader in a given term.

% The third possible outcome is that a candidate neither wins nor loses the election: if many followers become
% candidates at the same time, votes could be split so that no candidate obtains a majority. When this happens, each
% candidate will time out and start a new election by incrementing its term and initiating another round of RequestVote RPCs.
% However, without extra measures split votes could repeat indefinitely
candidate(cast, election_timeout, Data) ->
  io:format("[~p] the node ~p got election_timeout as candidate. this node fail to win leader election. so it turns to follower. ~n", [self(), my_name()]),
  NextEvent = next_event(cast, start_leader_election),
  {keep_state, Data, NextEvent};

% While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
% If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
% then the candidate recognizes the leader as legitimate and returns to follower state.
% If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
candidate(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, FromNodeName},
    #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

candidate(cast, {append_entries, AppendEntriesRpc, FromNodeName},
          #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {append_entries, AppendEntriesRpc, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

candidate(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=_LeaderName}=AppendEntriesRpc, _FromNodeName},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  Data1 = step_down(AppendEntriesTerm, Data0, candidate),
  {next_state, follower, Data1, {next_event, cast, {append_entries, AppendEntriesRpc}}};
candidate(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc, _FromNodeName},
          #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got invalid append_entries from ~p. thus, refuse it. ~n", [self(), my_name(), LeaderName]),
  AckAppendEntries = raft_rpc_append_entries:new_ack_fail_with_default(my_name(), CurrenTerm),
  AckMsg = raft_rpc_append_entries:new_ack_append_entries_msg(AckAppendEntries),
  ToPid = raft_util:get_node_pid(LeaderName),
  gen_statem:cast(ToPid, AckMsg),
  {keep_state, Data0};
candidate(cast, {append_entries, AppendEntries, _FromNodeName}, Data0) ->
  %% For example, add commited log its local state.
  Data1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  NewTerm = raft_rpc_append_entries:get(term, AppendEntries),
  Data2 = Data1#raft_state{current_term=NewTerm},
  {next_state, follower, Data2};

candidate(cast, {ack_append_entries, AckAppendEntries, FromNodeName},
    #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_append_entries, AckAppendEntries, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

candidate(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}, _FromNodeName},
          #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  step_down(NodeTerm, Data0, candidate);

% The third possible outcome is that a candidate neither wins nor loses the election: if many followers become candidates at the same time,
% votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election by incrementing
% its term and initiating another round of RequestVote RPCs. However, without extra measures split votes could repeat indefinitely.

% Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly. To prevent split votes in the first place, election timeouts are
% chosen randomly from a fixed interval (e.g., 150–300ms). This spreads out the servers so that in most cases only a single server will time out; it wins the election and sends
% heartbeats before any other servers time out. The same mechanism is used to handle split votes. Each candidate restarts its randomized election timeout at the start of an
% election, and it waits for that timeout to elapse before starting the next election; this reduces the likelihood of another split vote in the new election. Section 9.3 shows
% that this approach elects a leader rapidly.


candidate(EventType, EventContent, Data) ->
  raft_api:handle_event(EventType, EventContent, candidate, Data).

%% Append Entries (LEADER Implementation) -> Normally async.
%% Arguments:
%%  term: leader’s term
%%  leaderId: so follower can redirect clients
%%  prevLogIndex: index of log entry immediately preceding new ones
%%  prevLogTerm:  term of prevLogIndex entry
%%  entries[]: log entries to store (empty for heartbeat; may send more than one for efficiency)
%%  leaderCommit leader’s commitIndex
%% Results:
%%  term: currentTerm, for leader to update itself
%%  success: true if follower contained entry matching prevLogIndex and prevLogTerm
leader(cast, deprecated, Data0) ->
  {next_state, deprecated, Data0};

leader(cast, do_append_entries, Data0) ->
  io:format("[~p] Node ~p got do_append_entires ~n", [self(), my_name()]),

  Data1 = raft_scheduler:schedule_append_entries(Data0),
  #raft_state{members=Members} = Data1,
  MembersExceptMe = raft_util:members_except_me(Members),

  #raft_state{match_index=MatchIndex, rpc_due=RpcDue0,
              log_entries=LogEntries, next_index=NextIndex,
              current_term=CurrentTerm, commit_index=CommitIndex} = Data0,

  RpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex,
                                                      CurrentTerm, CommitIndex, RpcDue0),
  Data2 = Data1#raft_state{rpc_due=RpcDue},
  {keep_state, Data2};

leader(cast, {append_entries, AppendEntriesRpc, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {append_entries, AppendEntriesRpc, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

leader(cast, {append_entries, #append_entries{term=AppendEntriesTerm}=_AppendEntriesRpc, _FromNodeName},
       #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  io:format("[~p] node ~p append_entries ->step_down~n", [self(), my_name()]),
  step_down(AppendEntriesTerm, Data0, leader);

leader(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc, _FromNodeName},
       #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries. ~n", [self(), my_name()]),

  ToPid = raft_util:get_node_pid(LeaderName),
  AckAppendEntries = raft_rpc_append_entries:new_ack_fail_with_default(my_name(), CurrenTerm),
  AckMsg = raft_rpc_append_entries:new_ack_append_entries_msg(AckAppendEntries),
  gen_statem:cast(ToPid, AckMsg),
  {keep_state, Data0};

leader(cast, {ack_append_entries, AckAppendEntries, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_append_entries, AckAppendEntries, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

leader(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}, _FromNodeName},
       #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  io:format("[~p] node ~p ack_append_entries ->step_down~n", [self(), my_name()]),
  step_down(NodeTerm, Data0, leader);

leader(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}=AckAppendEntries, FromNodeName},
       #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm =:= NodeTerm ->
  #ack_append_entries{success=Success, node_name=NodeName, result=AppendEntriesResult} = AckAppendEntries,
  io:format("[~p] Node ~p got ack_append_entries. NodeName:~p ~n", [self(), my_name(), NodeName]),
  #raft_state{match_index=MatchIndex0, next_index=NextIndex0, commit_index=CommitIndex0,
              members=Members, log_entries=LogEntries, data=AppliedData0} = Data0,

  {MatchIndex, NextIndex, CommitIndex, AppliedData} =
    case Success of
      true ->
        #success_append_entries{match_index=NodeMatchIndex} = AppendEntriesResult,
        MatchIndex1 = maps:put(NodeName, NodeMatchIndex, MatchIndex0),
        NextIndex1 = maps:put(NodeName, NodeMatchIndex + 1, NextIndex0),
        {MaybeNewCommitIndex, MaybeNewAppliedData} =
          case raft_rpc_append_entries:commit_if_can(MatchIndex1, Members, CommitIndex0, LogEntries, CurrentTerm) of
            {true, MaybeNewCommitIndex0} ->
              ReversedLogEntries = lists:reverse(LogEntries),
              MaybeNewAppliedData0 = safe_get_entry_at_index(ReversedLogEntries, MaybeNewCommitIndex0, AppliedData0),
              {MaybeNewCommitIndex0, MaybeNewAppliedData0};
            {false, _ShouldIgnoreCommitIndex} ->
              {CommitIndex0, AppliedData0}
          end,

        {MatchIndex1, NextIndex1, MaybeNewCommitIndex, MaybeNewAppliedData};
      false ->
        #fail_append_entries{conflict_term=ConflictTerm, first_index_with_conflict_term=FirstIndexConflictTerm} = AppendEntriesResult,
        {NewNextIndexForPeer, NewMatchIndexForPeer} =
          case raft_rpc_append_entries:find_last_index_with_same_term(ConflictTerm, LogEntries) of
            % If there is entry that is same with conflict term in leader's entries,
            % Assume that FoundIndex is matched state, because the RAFT algorithm consider both `term` and `index`.
            % So, If two logs in different node has same index and same term, it is considered same entry.
            {true, FoundIndex} ->
              NewNextIndexForPeer1 = FoundIndex + 1,
              NewMatchIndexForPeer1 = FoundIndex,
              {NewNextIndexForPeer1, NewMatchIndexForPeer1};
            % If there is no entry that is same with conflict term in leader's entries,
            % Leader should use FirstIndexConflictTerm from follower.
            {false, _FoundIndex} ->
              NewNextIndexForPeer2 = FirstIndexConflictTerm,
              NewMatchIndexForPeer2 = max(0, FirstIndexConflictTerm - 1),
              {NewNextIndexForPeer2, NewMatchIndexForPeer2}
          end,

        MatchIndexInCaseOfFalse = maps:put(NodeName, NewMatchIndexForPeer, MatchIndex0),
        NextIndexInCaseOfFalse = maps:put(NodeName, NewNextIndexForPeer, MatchIndex0),

        {MatchIndexInCaseOfFalse, NextIndexInCaseOfFalse, CommitIndex0, AppliedData0}
    end,

  Data1 = Data0#raft_state{next_index=NextIndex, match_index=MatchIndex, commit_index=CommitIndex, data=AppliedData},

  %%% When leader commit to new index, that index become consensus.
  %%% So, After being changed from Cnew+old to Cnew,
  %%% leader don't send any RPCs to Cold. This is because
  %%% Cnew is already consensused. so we don't care Cold anymore even if Cold cannot get last leader commit which indicates {confirm_new_membership, ...}.
  ShouldHandleEntries = raft_util:get_entry(CommitIndex0, CommitIndex, lists:reverse(LogEntries)),
  Data2 = raft_cluster_change:handle_cluster_change_if_needed(ShouldHandleEntries, true, Data1),
  {keep_state, Data2};

leader(cast, {ack_append_entries, _, _FromNodeName}, Data0) ->
  % Ignore
  {keep_state, Data0};

leader(cast, election_timeout, Data) ->
  {keep_state, Data};

leader(cast, {request_vote, VoteArgs, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {request_vote, VoteArgs, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

leader(cast, {request_vote, VoteArgs, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {request_vote, VoteArgs, undefined}),
  handle_ignore_if_need_or_next(NextEventIfNotIgnorePeer, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

leader(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs, FromName},
       #raft_state{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  NextEvent = next_event(cast, {request_vote, VoteArgs, FromName}),
  io:format("[~p] node ~p request_vote ->step_down~n", [self(), my_name()]),
  step_down(CandidateTerm, Data0, NextEvent, leader);

leader(cast, {request_vote, VoteArgs, _FromName}, Data0) ->
  %%% Leader will not vote to other.
  %%% Because, Leader already has been voted itself.
  raft_rpc_request_vote:handle_request_vote_rpc(Data0, VoteArgs);

leader(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  NextEventIfNotIgnorePeer = next_event(cast, {ack_request_voted, FromName, ResponseTerm, VoteGranted, undefined}),
  handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEventIfNotIgnorePeer);

leader(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted, _FromNodeName}, Data0) ->
  #raft_state{current_term=CurrentTerm}=Data0,
  case CurrentTerm < ResponseTerm of
    true ->
      io:format("[~p] node ~p ack_request_voted ->step_down~n", [self(), my_name()]),
      step_down(ResponseTerm, Data0, leader);
    false -> {keep_state, Data0}
  end;

leader(cast, {new_entry, Entry, FromNodeName},
       #raft_state{ignore_peer=IgnorePeerName}=Data0) when FromNodeName =/= undefined ->
  case lists:member(FromNodeName, IgnorePeerName) of
    true ->
      io:format("[~p] Node ~p got new_entry from ~p, but ignore. ~n", [self(), my_name(), FromNodeName]),
      {keep_state, Data0};
    false ->
      % In this case, `undefined` is for workaround to filter testcodes.
      NextEvent = next_event(cast, {new_entry, Entry, undefined}),
      {keep_state, Data0, NextEvent}
  end;

leader(cast, {new_entry, Entry, _From}, Data0) ->
  io:format("[~p] node ~p got new entry, Entry: ~p~n,", [self(), my_name(), Entry]),
  #raft_state{log_entries=Entries0, current_term=CurrentTerm, next_index=NextIndex0, match_index=MatchIndex0} = Data0,
  NewEntry0 = {CurrentTerm, Entry},
  {Data1, NewEntry1} = raft_cluster_change:handle_cluster_change_immediately(NewEntry0, Data0),

  Entries = [NewEntry1| Entries0],
  NextMatchIndex = maps:get(my_name(), MatchIndex0) + 1,
  MatchIndex1 = maps:put(my_name(), NextMatchIndex, MatchIndex0),
  NextIndex1 = maps:put(my_name(), NextMatchIndex+1, NextIndex0),

  Data2 = Data1#raft_state{log_entries=Entries, match_index=MatchIndex1, next_index=NextIndex1},
  {keep_state, Data2};

leader(EventType, EventContent, Data) ->
  raft_api:handle_event(EventType, EventContent, leader, Data).

deprecated(EventType, EventContent, Data) ->
  raft_api:handle_event(EventType, EventContent, deprecated, Data).

handle_ignore_if_need_or_next(FromNodeName, IgnorePeerName, Data0, NextEvent) ->
  io:format("[~p] node ~p, invalid request_vote~n", [self(), my_name()]),
  case lists:member(FromNodeName, IgnorePeerName) of
    true ->
      io:format("[~p] Node ~p got request_vote from ~p, but ignore. ~n", [self(), my_name(), FromNodeName]),
      {keep_state, Data0};
    false ->
      % In this case, `undefined` is for workaround to filter testcodes.
      {keep_state, Data0, NextEvent}
  end.

clear_vote_granted(Data0)->
  Data0#raft_state{vote_granted=new_vote_granted()}.

new_vote_granted() ->
  #vote_granted{new_members=sets:new(), old_members=sets:new()}.

my_name() ->
  raft_util:my_name().

step_down(NewTerm, Data0, CurrentState) ->
  io:format("[~p] node ~p was triggered to step down. ~p -> follower ~n", [self(), my_name(), CurrentState]),
  Data1 = Data0#raft_state{current_term=NewTerm,
                           voted_for=undefined,
                           vote_granted=new_vote_granted()},
  {next_state, follower, Data1}.

step_down(NewTerm, Data0, NextEvent, CurrentState) ->
  io:format("[~p] node ~p was triggered to step down. ~p -> follower ~n", [self(), my_name(), CurrentState]),
  Data1 = Data0#raft_state{current_term=NewTerm,
                           voted_for=undefined,
                           vote_granted=new_vote_granted()},
  {next_state, follower, Data1, NextEvent}.

next_event(CastOrCall, Args) ->
  {next_event, CastOrCall, Args}.

init_rpc_due(Members) ->
  MustExpiredTime = 0,
  UnionMembers = raft_util:all_members(Members),
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, MustExpiredTime, Acc)
            end, #{}, UnionMembers).

init_match_index(Members) ->
  UnionMembers = raft_util:all_members(Members),
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, 0, Acc)
            end, #{}, UnionMembers).

init_next_index(Members) ->
  UnionMembers = raft_util:all_members(Members),
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, 1, Acc)
            end, #{}, UnionMembers).

log_term([], _Index) ->
  0;
log_term(LogEntries, Index) when length(LogEntries) < Index ->
  0;
log_term([Head|_Tail], _Index) ->
  {Term, _Log} = Head,
  Term.

safe_get_entry_at_index(LogEntries, Index, DefaultValue) ->
  try
    {_Term, Data} = lists:nth(Index, LogEntries),
    Data
  catch
    _:_  -> DefaultValue
  end.

reply(Msg, From) when is_pid(From)->
  From ! Msg;
reply(Msg, FromName) ->
  From = raft_util:get_node_pid(FromName),
  reply(Msg, From).


%% @private
%% TODO : Declare spec
apply_entries_to_state([], _FirstIndexInCurrentEntries, _PreviousCommitIndex, _NewCommitIndex, LocalRaftState, LastAppliedEntry) ->
  {LocalRaftState, LastAppliedEntry};
apply_entries_to_state(Entries, FirstIndexInCurrentEntries, PreviousCommitIndex, NewCommitIndex, LocalRaftState0, LastAppliedEntry) ->
  PreviousCommitIndexInEntries = in_entries(PreviousCommitIndex, FirstIndexInCurrentEntries),
  NewCommitIndexInEntries = in_entries(NewCommitIndex, FirstIndexInCurrentEntries),

  ReversedEntries = lists:reverse(Entries),
  EntriesToBeApplied = lists:sublist(ReversedEntries, PreviousCommitIndexInEntries, NewCommitIndexInEntries),

  case EntriesToBeApplied of
    [] -> {LocalRaftState0, LastAppliedEntry};
    EntriesToBeApplied ->
      % TODO : LastEntry는 Index를 포함하지 않음. 그걸 쓰도록 바꿔줘야함.
      [LastEntry|_] = lists:reverse(EntriesToBeApplied),
      RaftState = raft_command:commands(EntriesToBeApplied, LocalRaftState0),
      {RaftState, LastEntry}
  end.

%% @private
%% TODO : Declare Spec.
%% TODO : Put it into proper module. -> raft_log_compaction:handle(...)?
make_snapshot_if_needed(RaftState, RaftLogCompactionConfig0, CommitIndex, Entries0, LastAppliedEntries, RaftConfiguration) ->
  #raft_log_compaction_config{current_start_index=CurrentStartIndex} = RaftLogCompactionConfig0,
  #raft_configuration{snapshot_module=SnapshotModule, max_log_size=MaxLogSize} = RaftConfiguration,
  case length(Entries0) > MaxLogSize of
    false ->
      {RaftLogCompactionConfig0, Entries0};
    true ->
      CommitIndexInEntries = in_entries(CurrentStartIndex, CommitIndex),

      % TODO : LastEntry는 Index를 포함하지 않음. 그걸 쓰도록 바꿔줘야함.
      % LastIncludedIndex 확인
      % LastIncludeTerm 확인
      % LocalState 확인
      {LastIncludedTerm, LastIncludedIndex} = LastAppliedEntries,
      % LocalState, LastIncludedIndex, LastIncludedTerm
      NewSnapshot = SnapshotModule:create_snapshot(RaftState, LastIncludedIndex, LastIncludedTerm),
      SnapshotModule:upsert_snapshot(NewSnapshot),

      % TODO : 이 부분도 확인이 필요함. (Commit Index까지 반영했으니 Commit까지 짜른다)
      % 1. Start = CommitIndex + 1,  (CommitIndex가 0인 경우에는 아무것도 하지 않도록 하는 코너케이스)
      % 2. NewEntries = lists:sublist(Entries0, StartIndex), 같은 느낌?
      NewStartIndex = CommitIndexInEntries,
      NewEntries0 = Entries0,
      {NewStartIndex, NewEntries0}
  end.

%% @private
in_entries(DesiredIndex, StartIndex) ->
  DesiredIndex - StartIndex + 1.

