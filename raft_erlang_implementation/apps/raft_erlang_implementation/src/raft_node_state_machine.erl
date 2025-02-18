-module(raft_node_state_machine).

-behavior(gen_statem).

%%% Reference : https://raft.github.io/raft.pdf

%% API
-export([init/1]).
-export([start/2]).
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
  gen_statem:call(Pid, {request_vote, VoteArgs});
request_vote(Pid, VoteArgs) when is_pid(Pid)->
  gen_statem:call(Pid, {request_vote, VoteArgs}).

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


% TODO: Update event content.

follower({call, From}, {append_entries, Entries}, Data0) ->

  %% TODO : Implement Detail.
  ok;

follower(cast, election_timeout, Data0) ->
  io:format("[~p] Node ~p got election_timeout. from now on, it is candidate follower.~n", [self(), self()]),
  Timer = schedule_heartbeat_timeout(),
  Data1 = replace_append_entry_timer(Data0, Timer),
  Data2 = clear_given_voted(Data1),
  Data3 = increase_current_term(Data2),
  Data4 = vote(self(), Data3),
  {next_state, candidate, Data4, {next_event, cast, start_leader_election}};

follower({cast, From}, {promote, Args}, Data) ->
  io:format("[~p] Node ~p got promote but it's follower. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

follower(cast, {request_vote, {CandidateTerm, CandidateName, LastLogIndex, LastLogTerm}=VoteArgs}, Data0) ->
  #?MODULE{current_term=CurrentTerm, voted_for=VotedFor, last_applied=LastAppliedIndex} = Data0,

  Data1 =
    case raft_leader_election:can_vote(VotedFor, CurrentTerm, LastAppliedIndex, VoteArgs) of
      true ->
        % TODO: Send vote reply

        % TODO: Term 업데이트 하는게 맞는지 확인 필요.
        Data0#?MODULE{current_term=CandidateTerm, voted_for=CandidateName};
      false ->
        Data0
    end,
  % TODO : Detail implementation.
  {keep_state, Data1};

follower(EventType, EventContent, Data) ->
  io:format("Maybe here?
  EventType: ~p
  EventContent: ~p
  Data: ~p
  ~n", [EventType, EventContent, Data]),
  handle_event(EventType, EventContent, follower, Data).

%%(a) it wins the election,
%%(b) another server establishes itself as leader
%%(c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below

% TODO: Update event content.
candidate(cast, start_leader_election, Data0) ->
  % Should consider deadlock?
  #?MODULE{members=Members, current_term=CurTerm,
           given_voted=GivenVoted,
           last_applied=LastAppliedIndex,
           last_log_term=LastLogTerm} = Data0,

  case raft_leader_election:is_win(sets:size(Members), sets:size(GivenVoted)) of
    true -> {next_state, leader, Data0};
    false ->
      ToMembers = get_members_except_me(Members),
      VoteArgs = raft_leader_election:new_vote_arguments(self(), CurTerm, LastAppliedIndex, LastLogTerm),
      lists:foreach(
        fun(MemberPid) ->
          try
            request_vote(MemberPid, VoteArgs)
          catch
             exit:{noproc, _}  -> io:format("There is no such registred process ~p yet. ~n", [MemberPid])
          end
        end, sets:to_list(ToMembers)),

      io:format("START_LEADER_ELECTION !!!!!~n", []),
      {keep_state, Data0}
  end;

candidate({call, From}, {voted, FromName}, Data0) ->
  % TODO : 뒤늦게 응답이 오는 경우도 있을 수 있음. 그때는 무시해야함.
  {keep_state, Data0, [{reply, From, candidate}]};

candidate({call, From}, {append_entries, Entries}, Data0) ->
  %% TODO : Implement Detail.
  {next_state, follower, Data0, [{reply, From, follower}]};

candidate({call, From}, election_timeout, Data0) ->
  io:format("[~p] Node ~p got leader_election but it is already candidate. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Should be ignore
  {keep_state, Data0, [{reply, From, candidate}]};

% While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
% If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
% then the candidate recognizes the leader as legitimate and returns to follower state.
% If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
candidate(cast, {append_entries, NewLogs}, Data0) ->
  % TODO: Implement detail in terms of append entries.
  {next_state, follower, Data0};

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

my_name() ->
  whereis(self()).


%%% Schedule function.
jitter_election_timeout() ->
  Jitter = rand:uniform(150),
  150 + Jitter.

schedule_heartbeat_timeout() ->
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Timer.

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

get_node_pid(NodeName) ->
  whereis(NodeName).
