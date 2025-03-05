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

-type raft_state() :: follower   |
                      leader     |
                      candidate.

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
init({NodeName, Members}) ->

  erlang:register(NodeName, self()),
  % All node will be started with `follower` state.
  State = follower,

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
% TODO : implement redirect

% TODO: Update event content.

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

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm}=AppendEntriesRpc},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries1. ~n", [self(), my_name()]),
  Data1 = step_down(AppendEntriesTerm, Data0),
  {keep_state, Data1, {next_event, cast, {append_entries, AppendEntriesRpc}}};

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries2. ~n", [self(), my_name()]),
  AckAppendEntriesMsg = {my_name(), CurrenTerm, false, -1},
  ToPid = raft_util:get_node_pid(LeaderName),
  gen_statem:cast(ToPid, AckAppendEntriesMsg),
  {keep_state, Data0};

follower(cast, {append_entries, AppendEntriesRpc}, Data0)  ->

  Data1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  #raft_state{log_entries=Logs, current_term=CurrentTerm, commit_index=CommitIndex0,
              data=AppliedData0} = Data1,
  #append_entries{leader_name=LeaderName,
                  entries=LogsFromLeader,
                  leader_commit_index=LeaderCommitIndex,
                  previous_log_index=PrevLogIndex,
                  previous_log_term=PrevLogTerm} = AppendEntriesRpc,

  AppliedData = safe_get_entry_at_index(Logs, LeaderCommitIndex, AppliedData0),
  io:format("[~p] Node ~p got append_entries3 from ~p. ~n", [self(), my_name(), LeaderName]),
  NewLeader = LeaderName,
  IsSuccess = raft_rpc_append_entries:should_append_entries(PrevLogIndex, PrevLogTerm, Logs),
  {UpdatedLogEntries, CommitIndex, AckAppendEntries}
    = case IsSuccess of
            true ->
              %% Since the leader has declared that the index up to leaderCommit is “already safely replicated”, we can assume that the follower can commit up to that index.
              %% However, it is possible that the follower has not physically received logs up to that index yet, meaning that the actual length of the follower's logs (MatchIndex0) may be less than leaderCommit.
              %% Therefore, the follower can't commit to an index it doesn't actually have, so it must eventually commit up to the value of min(leaderCommit, the actual last index).
              {UpdatedLogEntries0, MatchIndex0} = raft_rpc_append_entries:do_concat_entries(Logs, LogsFromLeader, PrevLogIndex),
              CommitIndex1 = min(LeaderCommitIndex, MatchIndex0),
              SuccessAppendEntries = raft_rpc_append_entries:new_ack_success(my_name(), CurrentTerm, MatchIndex0),
              {UpdatedLogEntries0, CommitIndex1, SuccessAppendEntries};
            false ->
              {ConflictTerm, FoundFirstIndexWithConflictTerm} = raft_rpc_append_entries:find_earliest_index_at_conflict_term(PrevLogIndex, Logs),
              FailAppendEntries = raft_rpc_append_entries:new_ack_fail(my_name(), CurrentTerm, ConflictTerm, FoundFirstIndexWithConflictTerm),
              {Logs, CommitIndex0, FailAppendEntries}
          end,

  ToPid = raft_util:get_node_pid(LeaderName),
  Msg = {ack_append_entries, AckAppendEntries},
  gen_statem:cast(ToPid, Msg),

  Data2 = Data1#raft_state{leader=NewLeader, commit_index=CommitIndex,
                           log_entries=UpdatedLogEntries, data=AppliedData},

  ShouldHandleEntries = raft_util:get_entry(CommitIndex0, CommitIndex, UpdatedLogEntries),
  Data3 = raft_cluster_change:handle_cluster_change_if_needed(ShouldHandleEntries, Data2),

  {keep_state, Data3};

follower(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}},
    #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  step_down(NodeTerm, Data0);

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

follower(cast, {request_vote, VoteArgs}, #raft_state{current_term=CurrentTerm0}=Data0) ->
  #vote_args{candidate_name=CandidateName, candidate_term=CandidateTerm} = VoteArgs,
  io:format("[~p] Node ~p got request_vote from ~p~n", [self(), my_name(), CandidateName]),

  Data1 =
    case CurrentTerm0 < CandidateTerm of
      true -> Data0#raft_state{current_term=CandidateTerm, voted_for=undefined};
      false -> Data0
    end,

  raft_rpc_request_vote:handle_request_vote_rpc(Data1, VoteArgs);

follower(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted},
    #raft_state{current_term=CurrentTerm}=Data0) ->
  io:format("[~p] Node ~p got ack_request_voted ~n", [self(), my_name()]),
  case CurrentTerm < ResponseTerm of
    true -> step_down(ResponseTerm, Data0);
    false -> {keep_state, Data0}
  end;

follower(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, follower, Data).

%% (a) it wins the election,
%% (b) another server establishes itself as leader
%% (c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below

% TODO: Update event content.
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

  VoteArgs = raft_leader_election:new_vote_arguments(my_name(), NewlyCurrentTerm, LastLogIndex, LastLogTerm),
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

candidate(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs},
          #raft_state{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  io:format("[~p] Node ~p got request_vote ~n", [self(), my_name()]),
  NextEvent = next_event(cast, {request_vote, VoteArgs}),
  step_down(CandidateTerm, Data0, NextEvent);

candidate(cast, {request_vote, VoteArgs}, Data0) ->
  io:format("[~p] Node ~p got request_vote ~n", [self(), my_name()]),
  %%% Candidate will not vote to other.
  %%% Because, Candidate already has been voted itself.
  raft_rpc_request_vote:handle_request_vote_rpc(Data0, VoteArgs);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true},
          #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < ResponseTerm->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  step_down(ResponseTerm, Data0);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  #raft_state{current_term=CurrentTerm}=Data0,
  Data =
    case ResponseTerm =:= CurrentTerm of
      true ->
        #raft_state{vote_granted=VoteGranted0, rpc_due=RpcDue0} = Data0,
        RpcDue1 = maps:put(FromName, raft_rpc_timer_utils:infinity_rpc_due(), RpcDue0),
        VoteGranted1 = sets:add_element(FromName, VoteGranted0),
        Data0#raft_state{vote_granted=VoteGranted1, rpc_due=RpcDue1};
      false -> Data0
    end,
  NextEvent = next_event(cast, can_become_leader),
  {keep_state, Data, NextEvent};

candidate(cast, {ack_request_voted, FromName, _ResponseTerm, false}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  {keep_state, Data0};

candidate(cast, can_become_leader, Data0) ->
  io:format("[~p] Node ~p got can_become_leader ~n", [self(), my_name()]),
  #raft_state{vote_granted=VoteGranted, members=Members, current_term=CurrentTerm} = Data0,
  VoteGrantedSize = sets:size(VoteGranted),
  MemberSize = sets:size(Members),

  case raft_leader_election:has_quorum(MemberSize, VoteGrantedSize) of
    true ->
      io:format("[~p] the node ~p win the leader election. term is ~p.~n", [self(), my_name(), CurrentTerm]),
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
      {next_state, leader, Data1, NextEvent};
    false -> {keep_state, Data0}
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
candidate(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=_LeaderName}=AppendEntriesRpc},
         #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  Data1 = step_down(AppendEntriesTerm, Data0),
  {next_state, follower, Data1, {next_event, cast, {append_entries, AppendEntriesRpc}}};
candidate(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc},
          #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries. ~n", [self(), my_name()]),

  AckAppendEntries = raft_rpc_append_entries:new_ack_fail_with_default(my_name(), CurrenTerm),
  Msg = {ack_append_entries, AckAppendEntries},
  ToPid = raft_util:get_node_pid(LeaderName),
  gen_statem:cast(ToPid, Msg),
  {keep_state, Data0};
candidate(cast, {append_entries, AppendEntries}, Data0) ->
  %% TODO : Implement Detail.
  %% For example, add commited log its local state.
  Data1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  NewTerm = raft_rpc_append_entries:get(term, AppendEntries),
  Data2 = Data1#raft_state{current_term=NewTerm},
  {next_state, follower, Data2};

candidate(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}},
          #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  step_down(NodeTerm, Data0);

% The third possible outcome is that a candidate neither wins nor loses the election: if many followers become candidates at the same time,
% votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election by incrementing
% its term and initiating another round of RequestVote RPCs. However, without extra measures split votes could repeat indefinitely.

% Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly. To prevent split votes in the first place, election timeouts are
% chosen randomly from a fixed interval (e.g., 150–300ms). This spreads out the servers so that in most cases only a single server will time out; it wins the election and sends
% heartbeats before any other servers time out. The same mechanism is used to handle split votes. Each candidate restarts its randomized election timeout at the start of an
% election, and it waits for that timeout to elapse before starting the next election; this reduces the likelihood of another split vote in the new election. Section 9.3 shows
% that this approach elects a leader rapidly.


candidate(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, candidate, Data).

% TODO: Update event content.
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
leader(cast, do_append_entries, Data0) ->
  io:format("[~p] Node ~p got do_append_entires ~n", [self(), my_name()]),

  Data1 = raft_scheduler:schedule_append_entries(Data0),
  #raft_state{members=Members} = Data1,
  MembersExceptMe = sets:to_list(sets:del_element(my_name(), Members)),

  #raft_state{match_index=MatchIndex, rpc_due=RpcDue0, log_entries=LogEntries, next_index=NextIndex,
           current_term=CurrentTerm, commit_index=CommitIndex} = Data0,

  RpcDue = raft_rpc_append_entries:do_append_entries(MembersExceptMe, MatchIndex, LogEntries, NextIndex,
                                                      CurrentTerm, CommitIndex, RpcDue0),
  Data2 = Data1#raft_state{rpc_due=RpcDue},
  {keep_state, Data2};

leader(cast, {append_entries, #append_entries{term=AppendEntriesTerm}=AppendEntriesRpc},
       #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  Data1 = step_down(AppendEntriesTerm, Data0),
  {next_state, follower, Data1, {next_event, cast, {append_entries, AppendEntriesRpc}}};

leader(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=_AppendEntriesRpc},
       #raft_state{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries. ~n", [self(), my_name()]),

  ToPid = raft_util:get_node_pid(LeaderName),
  AckAppendEntries = raft_rpc_append_entries:new_ack_fail_with_default(my_name(), CurrenTerm),
  Msg = {ack_append_entries, AckAppendEntries},
  gen_statem:cast(ToPid, Msg),
  {keep_state, Data0};

leader(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}},
       #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm < NodeTerm ->
  step_down(NodeTerm, Data0);

leader(cast, {ack_append_entries, #ack_append_entries{node_term=NodeTerm}=AckAppendEntries},
       #raft_state{current_term=CurrentTerm}=Data0) when CurrentTerm =:= NodeTerm ->
  io:format("[~p] Node ~p got ack_append_entries. ~n", [self(), my_name()]),
  #ack_append_entries{success=Success, node_name=NodeName, result=AppendEntriesResult} = AckAppendEntries,
  #raft_state{match_index=MatchIndex0, next_index=NextIndex0, commit_index=CommitIndex0,
              members=Members, log_entries=LogEntries, data=AppliedData0} = Data0,

  {MatchIndex, NextIndex, CommitIndex, AppliedData} =
    case Success of
      true ->
        #success_append_entries{match_index=NodeMatchIndex} = AppendEntriesResult,
        MatchIndex1 = maps:put(NodeName, NodeMatchIndex, MatchIndex0),
        NextIndex1 = maps:put(NodeName, NodeMatchIndex + 1, NextIndex0),
        {MaybeNewCommitIndex, MaybeNewAppliedData} =
          case raft_rpc_append_entries:commit_if_can(MatchIndex1, sets:size(Members), CommitIndex0, LogEntries, CurrentTerm) of
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

  ShouldHandleEntries = raft_util:get_entry(CommitIndex0, CommitIndex, lists:reverse(LogEntries)),
  Data2 = raft_cluster_change:handle_cluster_change_if_needed(ShouldHandleEntries, Data1),
  {keep_state, Data2};

leader(cast, {ack_append_entries, _}, Data0) ->
  % Ignore
  {keep_state, Data0};

leader(cast, election_timeout, Data) ->
  {keep_state, Data};

leader(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs},
    #raft_state{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  NextEvent = next_event(cast, {request_vote, VoteArgs}),
  step_down(CandidateTerm, Data0, NextEvent);

leader(cast, {request_vote, VoteArgs}, Data0) ->
  %%% Leader will not vote to other.
  %%% Because, Leader already has been voted itself.
  raft_rpc_request_vote:handle_request_vote_rpc(Data0, VoteArgs);

leader(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted}, Data0) ->
  #raft_state{current_term=CurrentTerm}=Data0,
  %%% TODO : Some Followers can voted after for candidate to become leader.
  case CurrentTerm < ResponseTerm of
    true -> step_down(ResponseTerm, Data0);
    false -> {keep_state, Data0}
  end;

leader({call, From}, {new_entry, Entry}, Data0) ->
  io:format("[~p] Node ~p got new_entry ~p~n", [self(), my_name(), Entry]),
  #raft_state{log_entries=Entries0, current_term=CurrentTerm} = Data0,
  Entries = [{CurrentTerm, Entry}| Entries0],
  Data1 = Data0#raft_state{log_entries=Entries},
  io:format("Entries: ~p
  Data1: ~p~n", [Entries, Data1]),
  {keep_state, Data1, [{reply, From, success}]};

leader(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, leader, Data).

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
  VotedCount = sets:size(VoteGranted),
  {keep_state, Data, [{reply, From, VotedCount}]};

handle_event({call, From}, get_current_term, _State, Data) ->
  #raft_state{current_term=CurrentTerm} = Data,
  {keep_state, Data, [{reply, From, CurrentTerm}]};

handle_event({call, From}, get_voted_for, _State, Data) ->
  #raft_state{voted_for=VotedFor} = Data,
  {keep_state, Data, [{reply, From, VotedFor}]};

handle_event(_EventType, _EventCount, _State, Data) ->
  %% Ignore all other events.
  {keep_state, Data}.

clear_vote_granted(Data0)->
  Data0#raft_state{vote_granted=sets:new()}.

my_name() ->
  raft_util:my_name().

step_down(NewTerm, Data0) ->
  Data1 = Data0#raft_state{current_term=NewTerm,
                        voted_for=undefined,
                        vote_granted=sets:new()},
  {next_state, follower, Data1}.

step_down(NewTerm, Data0, NextEvent) ->
  Data1 = Data0#raft_state{current_term=NewTerm,
                        voted_for=undefined,
                        vote_granted=sets:new()},
  {next_state, follower, Data1, NextEvent}.

next_event(CastOrCall, Args) ->
  {next_event, CastOrCall, Args}.

init_rpc_due(Members) ->
  MustExpiredTime = 0,
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, MustExpiredTime, Acc)
            end, #{}, Members).

init_match_index(Members) ->
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, 0, Acc)
            end, #{}, Members).

init_next_index(Members) ->
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, 1, Acc)
            end, #{}, Members).

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