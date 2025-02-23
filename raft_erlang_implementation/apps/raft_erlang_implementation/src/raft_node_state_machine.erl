-module(raft_node_state_machine).

-behavior(gen_statem).

%%% Reference : https://raft.github.io/raft.pdf
%%% Reference2 : https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
%%% pseudo code : https://github.com/ongardie/raft-pseudocode

-include("rpc_record.hrl").

%% API
-export([init/1]).
-export([start/2]).
-export([stop/1]).
-export([promote/2]).
-export([held_leader_election/2]).
-export([request_vote/2]).
-export([callback_mode/0]).


%% For Test API
-export([get_state/1]).
-export([get_voted_count/1]).
-export([get_current_term/1]).
-export([get_timer/1]).
-export([get_voted_for/1]).

%% State Function
-export([follower/3]).
-export([candidate/3]).
-export([leader/3]).

-type raft_state() ::
  follower |
  leader |
  candidate.

% unit : millisecond.
-define(RPC_TIMEOUT, 50).

%%%%%%%%%%%%%%%%%% NOTE %%%%%%%%%%%%%%%%%%%%%
% Follower remains in follower state as long as it receives valid RPCs from a leader or candidate.

-define(STATE, ?MODULE).

-record(?MODULE, {
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

  members = sets:new() :: sets:sets(),

  vote_granted = sets:new() :: sets:sets(),

  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  %%%%%%%%%%%%%%%%%%%%% CUSTOM %%%%%%%%%%%%%%%%%%%%%%
  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  election_timeout_timer = undefined,
  append_entries_timer = undefined
}).

%% Use State Function. Not a event handler.
callback_mode() ->
  state_functions.

%% API.
start(NodeName, Members) ->
  MemberSet = sets:from_list(Members),
  gen_statem:start(?MODULE, {NodeName, MemberSet}, []).

request_vote(NodeName, VoteArgs) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  % Should be cast. otherwise, deadlock occur.
  % (Candidate A wait ack_voted from B, B wait ack_voted_from A)
  gen_statem:cast(Pid, {request_vote, VoteArgs});
request_vote(Pid, VoteArgs) when is_pid(Pid)->
  gen_statem:cast(Pid, {request_vote, VoteArgs}).

promote(NodeName, VoteArgs) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {promote, VoteArgs});
promote(Pid, VoteArgs) when is_pid(Pid)->
  gen_statem:call(Pid, {promote, VoteArgs}).

held_leader_election(NodeName, VoteArgs) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  gen_statem:call(Pid, {leader_election, VoteArgs});
held_leader_election(Pid, VoteArgs) when is_pid(Pid)->
  gen_statem:call(Pid, {leader_election, VoteArgs}).

%%% For Test
get_timer(Pid) ->
  gen_statem:call(Pid, get_timer).

get_voted_for(Pid) ->
  gen_statem:call(Pid, get_voted_for).

get_state(Pid) ->
  gen_statem:call(Pid, get_state).

get_voted_count(Pid) ->
  gen_statem:call(Pid, get_voted_count).

get_current_term(Pid) ->
  gen_statem:call(Pid, get_current_term).

-define(ELECTION_TIMEOUT, 10000).

%%% Mandatory callback functions.
init({NodeName, Members}) ->

  erlang:register(NodeName, self()),
  % All node will be started with `follower` state.
  State = follower,

  MatchIndex = init_match_index(Members),
  NextIndex = init_next_index(Members),
  RpcDue = init_rpc_due(Members),

  Timer = schedule_heartbeat_timeout(),
  Data = #?MODULE{election_timeout_timer=Timer,
                  members=Members,
                  match_index=MatchIndex,
                  next_index=NextIndex,
                  rpc_due=RpcDue},
  {ok, State, Data}.

stop(NodeName) ->
  Pid = get_node_pid(NodeName),
  gen_statem:stop(Pid).

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

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=AppendEntriesRpc},
         #?MODULE{current_term=CurrenTerm}=Data0) when CurrenTerm < AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries1. ~n", [self(), my_name()]),
  Data1 = step_down(AppendEntriesTerm, Data0),
  Data2 = Data1#?MODULE{leader=LeaderName},
  {keep_state, Data2, {next_event, cast, {append_entries, AppendEntriesRpc}}};

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=AppendEntriesRpc},
         #?MODULE{current_term=CurrenTerm}=Data0) when CurrenTerm > AppendEntriesTerm ->
  io:format("[~p] Node ~p got append_entries2. ~n", [self(), my_name()]),
  AckAppendEntriesMsg = {my_name(), CurrenTerm, false},
  ToPid = get_node_pid(LeaderName),
  gen_statem:cast(ToPid, AckAppendEntriesMsg),
  {keep_state, Data0};

follower(cast, {append_entries, #append_entries{term=AppendEntriesTerm, leader_name=LeaderName}=AppendEntriesRpc},
         #?MODULE{current_term=CurrenTerm}=Data0)  ->
  io:format("[~p] Node ~p got append_entries3 from ~p. ~n", [self(), my_name(), LeaderName]),
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  % TODO : Implement Detail
  {keep_state, Data1};


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

follower(cast, {request_vote, VoteArgs}, #?MODULE{current_term=CurrentTerm0}=Data0) ->
  #vote_args{candidate_name=CandidateName, candidate_term=CandidateTerm} = VoteArgs,
  io:format("[~p] Node ~p got request_vote from ~p~n", [self(), my_name(), CandidateName]),

  Data1 =
    case CurrentTerm0 < CandidateTerm of
      true -> Data0#?MODULE{current_term=CandidateTerm, voted_for=undefined};
      false -> Data0
    end,

  handle_request_vote_rpc(Data1, VoteArgs);

follower(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted},
    #?MODULE{current_term=CurrentTerm}=Data0) ->
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
candidate(cast, start_leader_election, #?MODULE{current_term=CurrentTerm}=Data0) ->
  io:format("[~p] Node ~p start leader election. Term is ~p.~n", [self(), my_name(), CurrentTerm]),
  % Should consider deadlock.
  % So, we DO NOT USE syncronous function.
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  Data2 = clear_vote_granted(Data1),
  Data3 = vote_my_self(CurrentTerm + 1, Data2),

  #?MODULE{members=Members, current_term=NewlyCurrentTerm,
           log_entries=LogEntries} = Data3,

  RpcDue = init_rpc_due(Members),
  NextIndex = init_next_index(Members),
  MatchIndex = init_match_index(Members),
  Data4 = Data3#?MODULE{rpc_due=RpcDue, next_index=NextIndex, match_index=MatchIndex},

  LastLogIndex = length(LogEntries),
  LastLogTerm = log_term(LogEntries, LastLogIndex),

  VoteArgs = raft_leader_election:new_vote_arguments(my_name(), NewlyCurrentTerm, LastLogIndex, LastLogTerm),
  RpcDueExceptMe = maps:filter(fun(Member, _Timeout) ->
                                  Member =/= my_name()
                                end, RpcDue),

  SendRequestVoteIfNodeIsExisted =
    fun(ToMemberName, _Timeout, Acc) ->
      ToMemberPid = get_node_pid(ToMemberName),
      try
        request_vote(ToMemberPid, VoteArgs),
        maps:put(ToMemberName, next_rpc_due(), Acc)
      catch
        exit:{noproc, _}  ->
          io:format("There is no such registred process ~p yet. ~n", [ToMemberPid]),
          Acc
      end
    end,
  NewRpcDue = maps:fold(SendRequestVoteIfNodeIsExisted, #{}, RpcDueExceptMe),
  Data5 = Data4#?MODULE{rpc_due=NewRpcDue},
  NextEvent = next_event(cast, can_become_leader),

  {keep_state, Data5, NextEvent};

candidate(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs},
          #?MODULE{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  io:format("[~p] Node ~p got request_vote ~n", [self(), my_name()]),
  NextEvent = next_event(cast, {request_vote, VoteArgs}),
  step_down(CandidateTerm, Data0, NextEvent);

candidate(cast, {request_vote, VoteArgs}, Data0) ->
  io:format("[~p] Node ~p got request_vote ~n", [self(), my_name()]),
  %%% Candidate will not vote to other.
  %%% Because, Candidate already has been voted itself.
  handle_request_vote_rpc(Data0, VoteArgs);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true},
          #?MODULE{current_term=CurrentTerm}=Data0) when CurrentTerm < ResponseTerm->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  step_down(ResponseTerm, Data0);

candidate(cast, {ack_request_voted, FromName, ResponseTerm, true}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  #?MODULE{current_term=CurrentTerm}=Data0,
  Data =
    case ResponseTerm =:= CurrentTerm of
      true ->
        #?MODULE{vote_granted=VoteGranted0, rpc_due=RpcDue0} = Data0,
        RpcDue1 = maps:put(FromName, infinity_rpc_due(), RpcDue0),
        VoteGranted1 = sets:add_element(FromName, VoteGranted0),
        Data0#?MODULE{vote_granted=VoteGranted1, rpc_due=RpcDue1};
      false -> Data0
    end,
  NextEvent = next_event(cast, can_become_leader),
  {keep_state, Data, NextEvent};

candidate(cast, {ack_request_voted, FromName, _ResponseTerm, false}, Data0) ->
  io:format("[~p] Node ~p got ack_request_voted from ~p~n", [self(), my_name(), FromName]),
  {keep_state, Data0};

candidate(cast, can_become_leader, Data0) ->
  io:format("[~p] Node ~p got can_become_leader ~n", [self(), my_name()]),
  #?MODULE{vote_granted=VoteGranted, members=Members, current_term=CurrentTerm} = Data0,
  VoteGrantedSize = sets:size(VoteGranted),
  MemberSize = sets:size(Members),

  case raft_leader_election:has_quorum(MemberSize, VoteGrantedSize) of
    true ->
      io:format("[~p] the node ~p win the leader election. term is ~p.~n", [self(), my_name(), CurrentTerm]),
      #?MODULE{next_index=NextIndex0, log_entries=Logs} = Data0,
      Length = length(Logs),
      NextIndex = maps:fold(fun(MemberName, _NextIndex, Acc) ->
                              maps:put(MemberName, Length + 1, Acc)
                            end, #{}, NextIndex0),
      InitRpcDue = init_rpc_due(Members),
      Data1 = Data0#?MODULE{leader=my_name(), next_index=NextIndex, rpc_due=InitRpcDue},
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
candidate(cast, {append_entries, #append_entries{term=LeaderTerm}}, #?MODULE{current_term=CandidateTerm}=Data0)
  when LeaderTerm < CandidateTerm ->
  {keep_state, Data0};
candidate(cast, {append_entries, AppendEntries}, Data0) ->
  %% TODO : Implement Detail.
  %% For example, add commited log its local state.
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  NewTerm = raft_rpc_append_entries:get(term, AppendEntries),

  Data2 = Data1#?MODULE{current_term=NewTerm},

  %%% TODO : 받았다는 응답을 Leader에게 보내줘야 할 수도 있음.
  %%% 그래야 Leader도 Commit을 할꺼니까.
  {next_state, follower, Data2};
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

  Data1 = schedule_append_entries(Data0),
  #?MODULE{members=Members} = Data1,
  MembersExceptMe = sets:to_list(sets:del_element(my_name(), Members)),
  Data2 = do_append_entries(MembersExceptMe, Data1),
  {keep_state, Data2};

leader(cast, election_timeout, Data) ->
  {keep_state, Data};

leader(cast, {request_vote, #vote_args{candidate_term=CandidateTerm}=VoteArgs},
    #?MODULE{current_term=CurrentTerm0}=Data0) when CurrentTerm0 < CandidateTerm->
  NextEvent = next_event(cast, {request_vote, VoteArgs}),
  step_down(CandidateTerm, Data0, NextEvent);

leader(cast, {request_vote, VoteArgs}, Data0) ->
  %%% Leader will not vote to other.
  %%% Because, Leader already has been voted itself.
  handle_request_vote_rpc(Data0, VoteArgs);

leader(cast, {ack_request_voted, _FromName, ResponseTerm, _Granted}, Data0) ->
  #?MODULE{current_term=CurrentTerm}=Data0,
  %%% TODO : Some Followers can voted after for candidate to become leader.
  case CurrentTerm < ResponseTerm of
    true -> step_down(ResponseTerm, Data0);
    false -> {keep_state, Data0}
  end;

leader(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, leader, Data).

%%{next_state, follower, Data, [{reply, From, candidate}]};
handle_event({call, From}, get_timer, State, Data) ->
  #?MODULE{election_timeout_timer=Timer}=Data,
  {keep_state, Data, [{reply, From, Timer}]};
handle_event({call, From}, get_state, State, Data) ->
  {keep_state, Data, [{reply, From, State}]};

handle_event({call, From}, get_voted_count, _State, Data) ->
  #?MODULE{vote_granted=VoteGranted} = Data,
  VotedCount = sets:size(VoteGranted),
  {keep_state, Data, [{reply, From, VotedCount}]};

handle_event({call, From}, get_current_term, _State, Data) ->
  #?MODULE{current_term=CurrentTerm} = Data,
  {keep_state, Data, [{reply, From, CurrentTerm}]};

handle_event({call, From}, get_voted_for, _State, Data) ->
  #?MODULE{voted_for=VotedFor} = Data,
  {keep_state, Data, [{reply, From, VotedFor}]};

handle_event(EventType, EventCount, State, Data) ->
  %% Ignore all other events.
  {keep_state, Data}.

clear_vote_granted(Data0)->
  Data0#?MODULE{vote_granted=sets:new()}.

vote_my_self(NewTerm, Data0) ->
  #?MODULE{vote_granted=VoteGranted0} = Data0,
  VoteGranted = sets:add_element(my_name(), VoteGranted0),
  Data0#?MODULE{vote_granted=VoteGranted, voted_for=my_name(), current_term=NewTerm}.


vote(CandidateName, NewTerm, Data0) ->
  #?MODULE{vote_granted=VoteGranted0} = Data0,
  VoteGranted = sets:add_element(CandidateName, VoteGranted0),
  Data0#?MODULE{vote_granted=VoteGranted, voted_for=CandidateName, current_term=NewTerm}.

%%  Results:
%%   term: currentTerm, for candidate to update itself
%%   voteGranted: true means candidate received vote

%% Receiver implementation:
%%  1. Reply false if term < currentTerm (§5.1)
%%  2. If votedFor is null or candidateId, and candidate’s log is at
%%     least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
ack_request_voted(CandidateName, CurrentTerm, VoteGranted) ->
  io:format("[~p] Node ~p send ack_request_voted to ~p~n", [self(), my_name(), CandidateName]),
  ToPid = get_node_pid(CandidateName),
  RequestVoteRpc = raft_rpc_request_vote:new_ack(CurrentTerm, VoteGranted, my_name()),
  gen_statem:cast(ToPid, RequestVoteRpc).

%%% Schedule function.
jitter_election_timeout() ->
  Jitter = 1.0 + rand:uniform(),
  FloatTimeout = raft_util:get_timer_time() * Jitter,
  trunc(FloatTimeout).

schedule_heartbeat_timeout() ->
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Timer.

schedule_heartbeat_timeout_and_cancel_previous_one(Data) ->
  #?MODULE{election_timeout_timer=PreviousTimer} = Data,
  timer:cancel(PreviousTimer),
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Data#?MODULE{election_timeout_timer=Timer}.

schedule_append_entries(Data0) ->
  NextScheduled = raft_util:get_election_timeout_divided_by(4),
  {ok, Timer} = timer:apply_after(NextScheduled, gen_statem, cast, [self(), do_append_entries]),
  Data0#?MODULE{append_entries_timer=Timer}.

get_node_name_by_pid(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.

my_name() ->
  node_name(self()).

node_name(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.

get_node_pid(NodeName) ->
  whereis(NodeName).

step_down(NewTerm, Data0) ->
  Data1 = Data0#?MODULE{current_term=NewTerm,
                        voted_for=undefined,
                        vote_granted=sets:new()},
  {next_state, follower, Data1}.

step_down(NewTerm, Data0, NextEvent) ->
  Data1 = Data0#?MODULE{current_term=NewTerm,
                        voted_for=undefined,
                        vote_granted=sets:new()},
  {next_state, follower, Data1, NextEvent}.

handle_request_vote_rpc(Data0, VoteArgs) ->
  #?MODULE{voted_for=VotedFor,
           current_term=CurrentTerm,
           last_applied=FollowerLogLastIndex,
           last_log_term=FollowerLogLastTerm} = Data0,

  #vote_args{candidate_name=CandidateName} = VoteArgs,

  case raft_leader_election:can_vote(VotedFor, CurrentTerm, FollowerLogLastTerm, FollowerLogLastIndex, VoteArgs) of
    true ->
      Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
      Data2 = vote(CandidateName, CurrentTerm, Data1),
      ack_request_voted(CandidateName, CurrentTerm, true),
      {keep_state, Data2};
    false ->
      ack_request_voted(CandidateName, CurrentTerm, false),
      {keep_state, Data0}
  end.

next_event(CastOrCall, Args) ->
  {next_event, CastOrCall, Args}.

current_time() ->
  os:system_time(millisecond).

is_rpc_expired(MemorizedTime) ->
  Now = current_time(),
  MemorizedTime < Now.

infinity_rpc_due() ->
  current_time() * 10.

next_rpc_due() ->
  current_time() + ?RPC_TIMEOUT.

next_rpc_due_divide_by(DivideNum) ->
  DividedRpcTimeout = ?RPC_TIMEOUT div DivideNum,
  current_time() + DividedRpcTimeout.

init_rpc_due(Members) ->
  MustExpiredTime = 0,
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, MustExpiredTime, Acc)
            end, #{}, Members).

% index of highest log entry known to be replicated on peer
% matchIndex[peer] := 0
init_match_index(Members) ->
  sets:fold(fun(Member, Acc) ->
              maps:put(Member, 0, Acc)
            end, #{}, Members).

% index of next log entry to send to peer
% nextIndex[peer] := 1
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


%%send AppendEntries to peer
%%   on (state == LEADER and
%%        (matchIndex[peer] < len(log) or
%%          rpcDue[peer] < now()):
%%   | rpcDue[peer] = now() + ELECTION_TIMEOUT / 2
%%   | prevIndex = nextIndex[peer] - 1
%%   | lastIndex := choose in (nextIndex[peer] - 1)..len(log)
%%   | nextIndex[peer] = lastIndex
%%   | send AppendEntries to peer {
%%   |   term: currentTerm,
%%   |   prevTerm: getTerm(prevIndex),
%%   |   entries: log[prevIndex+1..lastIndex],
%%   |   commitIndex: commitIndex}

do_append_entries([], Data) ->
  Data;
do_append_entries([Member|Rest], Data0) ->
  #?MODULE{match_index=MatchIndex, rpc_due=RpcDue0, log_entries=Log, next_index=NextIndex0,
          current_term=CurrentTerm, commit_index=CommitIndex} = Data0,
  MatchIndexOfMember = maps:get(Member, MatchIndex),
  HasLagOfLog = MatchIndexOfMember < length(Log),

  RpcDueOfMember = maps:get(Member, RpcDue0),
  IsRpcExpired = is_rpc_expired(RpcDueOfMember),

  case HasLagOfLog orelse IsRpcExpired of
    true ->
      % TODO HERE
      NextRpcExpiredTime = next_rpc_due_divide_by(2),
      RpcDue = maps:put(Member, NextRpcExpiredTime, RpcDue0),

      PrevIndex = maps:get(Member, NextIndex0) - 1,

      LastIndex = length(Log),
      NextIndex = maps:put(Member, LastIndex, NextIndex0),

      ToBeSentEntries = get_log_nth(Log, LastIndex - PrevIndex, []),

      PrevTerm = case ToBeSentEntries of
                   [] -> 0 ;
                   [Head0 | _Tail0] ->
                     {Term0, _Log0} = Head0,
                     Term0
                 end,

      AppendEntriesRpc = raft_rpc_append_entries:new(CurrentTerm, my_name(), PrevIndex, PrevTerm, ToBeSentEntries, CommitIndex),
      AppendEntriesRpcMsg = {append_entries, AppendEntriesRpc},
      ToMemberPid = get_node_pid(Member),
      gen_statem:cast(ToMemberPid, AppendEntriesRpcMsg),
      Data1 = Data0#?MODULE{rpc_due=RpcDue, next_index=NextIndex},
      do_append_entries(Rest, Data1);
    false ->
      do_append_entries(Rest, Data0)
  end.

get_log_nth([], N, Acc) ->
  Acc;
get_log_nth(Logs, N, Acc) when N =:= 0 ->
  Acc;
get_log_nth([H|T], N, Acc0) ->
  get_log_nth(T, N-1, [H|Acc0]).


