-module(raft_node_state_machine).

-behavior(gen_statem).

%%% Reference : https://raft.github.io/raft.pdf

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

%% State Function
-export([follower/3]).
-export([candidate/3]).
-export([leader/3]).

-type raft_state() ::
  follower |
  leader |
  candidate.

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

  % Custom attribute
  last_log_term = 0 :: integer(),

  %%% Volatile state on leaders (Reinitialized after election)
  % For each server, index of the next log entry to send to that server
  % (initialized to leader last log index + 1)
  next_index :: integer(),

  % for each server, index of highest log entry known to be replicated on server.
  % (initialized to 0, increases monotonically)
  match_index = 0 :: integer(),

  members = sets:new() :: sets:sets(),
  given_voted = sets:new() :: sets:sets(),
  append_entry_timer = undefined
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

  Timer = schedule_heartbeat_timeout(),
  Data = #?MODULE{append_entry_timer=Timer, members=Members},
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

follower({call, From}, {append_entries, Entries}, Data0) ->
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  %% TODO : Implement Detail.
  {keep_state, Data0};

follower(cast, election_timeout, Data0) ->
  io:format("[~p] Node ~p got election_timeout. from now on, it is candidate follower. leader election will be held in a few second. ~n", [self(), my_name()]),
  Data1 = schedule_heartbeat_timeout_and_replace_previous_one(Data0),
  Data2 = clear_given_voted(Data1),
  Data3 = increase_current_term(Data2),
  Data4 = vote(self(), Data3),
  {next_state, candidate, Data4, {next_event, cast, start_leader_election}};

follower({cast, From}, {promote, Args}, Data) ->
  io:format("[~p] Node ~p got promote but it's follower. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), my_name()]),
  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

follower(cast, {request_vote, {CandidateTerm, CandidateName, _LastLogIndex, LastLogTerm}=VoteArgs}, Data0) ->
  #?MODULE{current_term=CurrentTerm, voted_for=VotedFor, last_applied=LastAppliedIndex,
           last_log_term=LastLogTerm} = Data0,
  Data1 =
    case CurrentTerm =< CandidateTerm of
      % Ignore
      false -> ok;
      true ->
        case raft_leader_election:can_vote(VotedFor, LastLogTerm, LastAppliedIndex, VoteArgs) of
          true ->
            % TODO: Send vote reply
            ack_request_voted(CandidateName),
            % TODO: Term 업데이트 하는게 맞는지 확인 필요.
            Data0#?MODULE{current_term=CandidateTerm, voted_for=CandidateName};
          false -> Data0
        end
    end,
  % TODO : Detail implementation.
  {keep_state, Data1};

follower(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, follower, Data).

%%(a) it wins the election,
%%(b) another server establishes itself as leader
%%(c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below

% TODO: Update event content.
candidate(cast, start_leader_election, #?MODULE{current_term=CurrentTerm}=Data0) ->
  io:format("[~p] Node ~p start leader election. Term is ~p.~n", [self(), my_name(), CurrentTerm]),
  % Should consider deadlock.
  % So, we DO NOT USE syncronous function.
  #?MODULE{members=Members, current_term=CurTerm,
           given_voted=GivenVoted,
           last_applied=LastAppliedIndex,
           last_log_term=LastLogTerm} = Data0,

  case raft_leader_election:is_win(sets:size(Members), sets:size(GivenVoted)) of
    true -> {next_state, leader, Data0};
    false ->
      ToMembers = get_members_except_me(Members),
      VoteArgs = raft_leader_election:new_vote_arguments(my_name(), CurTerm, LastAppliedIndex, LastLogTerm),

      RequestVoteIfNodeIsExisted = fun(MemberPid) ->
                                     try
                                       request_vote(MemberPid, VoteArgs)
                                     catch
                                       exit:{noproc, _}  -> io:format("There is no such registred process ~p yet. ~n", [MemberPid])
                                     end
                                   end,
      lists:foreach(RequestVoteIfNodeIsExisted, sets:to_list(ToMembers)),
      {keep_state, Data0}
  end;

candidate(cast, {ack_request_voted, FromName}, Data0) ->
  io:format("[~p] the node ~p got ack_request_voted from ~p.~n", [self(), my_name(), FromName]),
  #?MODULE{given_voted=GivenVoted0, members=Members} = Data0,
  GivenVoted1 = sets:add_element(FromName, GivenVoted0),
  Data1 = Data0#?MODULE{given_voted=GivenVoted1},
  GivenVotedSize = sets:size(GivenVoted1),
  MemberSize = sets:size(Members),

  case raft_leader_election:is_win(MemberSize, GivenVotedSize) of
    true ->
      io:format("[~p] the node ~p win the leader election. ~n", [self(), my_name()]),
      {next_state, leader, Data1};
    false -> {keep_state, Data1}
  end;

% In some situations an election will result in a split vote. In this case the term
% will end with no leader; a new term (with a new election) will begin shortly.
% Raft ensures that there is at most one leader in a given term.
candidate(cast, election_timeout, Data0) ->
  io:format("[~p] the node ~p got election_timeout as candidate. this node fail to win leader election. so it turns to follower. ~n", [self(), my_name()]),
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  {next_state, follower, Data1};

% While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
% If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
% then the candidate recognizes the leader as legitimate and returns to follower state.
% If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
candidate({call, From}, {append_entries, Entries}, Data0) ->
  Data1 = schedule_heartbeat_timeout_and_cancel_previous_one(Data0),
  %% TODO : Implement Detail.
  {next_state, follower, Data0, [{reply, From, follower}]};

% The third possible outcome is that a candidate neither wins nor loses the election: if many followers become candidates at the same time,
% votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election by incrementing
% its term and initiating another round of RequestVote RPCs. However, without extra measures split votes could repeat indefinitely.

% Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly. To prevent split votes in the first place, election timeouts are
% chosen randomly from a fixed interval (e.g., 150–300ms). This spreads out the servers so that in most cases only a single server will time out; it wins the election and sends
% heartbeats before any other servers time out. The same mechanism is used to handle split votes. Each candidate restarts its randomized election timeout at the start of an
% election, and it waits for that timeout to elapse before starting the next election; this reduces the likelihood of another split vote in the new election. Section 9.3 shows
% that this approach elects a leader rapidly.


candidate({call, From}, {leader_election, Args}, Data) ->
  io:format("[~p] Node ~p got leader_election but it is already candidate. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  {next_state, leader, Data, [{reply, From, candidate}]};

candidate({call, From}, {promote, Args}, Data) ->
  io:format("[~p] Node ~p got promote. it wins the leader election. ~n", [self(), self()]),
  {next_state, leader, Data, [{reply, From, candidate}]};

candidate({call, From}, {request_vote, _VoteArgs}, Data) ->
  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

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
leader(cast, start_append_entries, Data0) ->
  ok;


leader({call, From}, election_timeout, Data0) ->
  io:format("[~p] Node ~p got leader_election but it is already candidate. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Should be ignore
  {keep_state, Data0, [{reply, From, candidate}]};

leader({call, From}, {leader_election, Args}, Data) ->
  io:format("[~p] Node ~p got leader_election but it's already leader. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  {keep_state, Data, [{reply, From, candidate}]};

leader({call, From}, {promote, Args}, Data) ->
  io:format("[~p] Node ~p got promote but it's leader. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  {keep_state, Data, [{reply, From, candidate}]};

leader({call, From}, {request_vote, _VoteArgs}, Data) ->
  io:format("[~p] Node ~p got request_vote but it's leader. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

leader(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, leader, Data).

%%{next_state, follower, Data, [{reply, From, candidate}]};
handle_event({call, From}, get_state, State, Data) ->
  {keep_state, Data, [{reply, From, State}]};

handle_event({call, From}, get_voted_count, _State, Data) ->
  #?MODULE{given_voted=Voted} = Data,
  VotedCount = sets:size(Voted),
  {keep_state, Data, [{reply, From, VotedCount}]};

handle_event({call, From}, get_current_term, _State, Data) ->
  #?MODULE{current_term=CurrentTerm} = Data,
  {keep_state, Data, [{reply, From, CurrentTerm}]};

handle_event(EventType, EventCount, State, Data) ->
  %% Ignore all other events.
  {keep_state, Data}.

clear_given_voted(Data0) ->
  Data0#?MODULE{given_voted=sets:new()}.

vote(NodeName, Data0) ->
  #?MODULE{given_voted=GivenVoted0} = Data0,
  GivenVoted = sets:add_element(NodeName, GivenVoted0),
  Data0#?MODULE{given_voted=GivenVoted, voted_for=NodeName}.

increase_current_term(Data0) ->
  #?MODULE{current_term=CurrentTerm} = Data0,
  Data0#?MODULE{current_term=CurrentTerm + 1}.

ack_request_voted(CandidateName) ->
  io:format("ack_request_voted CAndidateName: ~p~n", [CandidateName]),
  ToPid = get_node_pid(CandidateName),
  gen_statem:cast(ToPid, {ack_request_voted, my_name()}).

%%% Schedule function.
jitter_election_timeout() ->
  Jitter = rand:uniform(150),
  raft_util:get_timer_time() + Jitter.

schedule_heartbeat_timeout() ->
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Timer.

schedule_heartbeat_timeout_and_replace_previous_one(Data) ->
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  io:format("Timere here ~n", []),
  Data#?MODULE{append_entry_timer=Timer}.

schedule_heartbeat_timeout_and_cancel_previous_one(Data) ->
  #?MODULE{append_entry_timer=PreviousTimer} = Data,
  timer:cancel(PreviousTimer),
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Data#?MODULE{append_entry_timer=Timer}.

%%schedule_append_entries(Data) ->
%%  timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), {append_entries}])
%%  #?MODULE.






remove_append_entry_timer(Data0) ->
  Data0#?MODULE{append_entry_timer=undefined}.

replace_append_entry_timer(Data0, Timer) ->
  Data0#?MODULE{append_entry_timer=Timer}.

get_members_except_me(Members) ->
  MyName = get_node_name_by_pid(self()),
  sets:del_element(MyName, Members).

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
