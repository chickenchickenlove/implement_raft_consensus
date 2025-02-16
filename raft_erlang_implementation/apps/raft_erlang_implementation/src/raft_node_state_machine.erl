-module(raft_node_state_machine).

%% API
-export([promote/2]).
-export([held_leader_election/2]).
-export([request_vote/2]).

-behavior(gen_statem).

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
start() ->
  gen_statem:start(?MODULE, [], []).

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


-define(ELECTION_TIMEOUT, 10000).


%%% Mandatory callback functions.
init(_Args) ->
  % All node will be started with `follower` state.
  State = follower,
  Data = #?MODULE,
  {ok, State, Data}.


% TODO: Update event content.
follower({call, From}, append_entry_timeout, Data0) ->
  io:format("[~p] Node ~p got leader_election. from now on, it is candidate follower.~n", [self(), self()]),

  Data1 = remove_append_entry_timer(Data0),
  Data2 = clear_given_voted(Data1),
  Data3 = increase_current_term(Data2),
  Data4 = vote(my_name(), Data3),

  % TODO : Detail implementation.
  {next_state, candidate, Data4, [{reply, From, candidate}]};

follower({call, From}, {promote, Args}, Data) ->
  io:format("[~p] Node ~p got promote but it's follower. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

follower({call, From}, {request_vote, {CandidateTerm, CandidateName, LastLogIndex, LastLogTerm}}, Data0) ->
  #?MODULE{current_term=CurrentTerm, voted_for=VotedFor} = Data0,

  Data1 =
    case VotedFor of
      undefined ->


      ;
      AlreadyVotedTo -> ;



    end,



  % TODO : Detail implementation.
  {keep_state, Data, [{reply, From, candidate}]};

follower(EventType, EventContent, Data) ->
  handle_event(EventType, EventContent, Data).

% TODO: Update event content.
candidate({call, From}, {voted, FromName}, Data0) ->
  % TODO : 뒤늦게 응답이 오는 경우도 있을 수 있음. 그때는 무시해야함.
  {keep_state, Data0, [{reply, From, candidate}]};

candidate({call, From}, append_entry_timeout, Data0) ->
  io:format("[~p] Node ~p got leader_election but it is already candidate. it is in somewhat inconsistency. It will be eventually resolved. ~n", [self(), self()]),
  % TODO : Should be ignore
  {keep_state, Data0, [{reply, From, candidate}]};

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
  handle_event(EventType, EventContent, Data).

% TODO: Update event content.
leader({call, From}, append_entry_timeout, Data0) ->
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
  handle_event(EventType, EventContent, Data).

%%{next_state, follower, Data, [{reply, From, candidate}]};


handle_event({call, From}, get_count, Data) ->
  % For handling common events to all state.
  {keep_state, Data, [{reply, From, Data}]};
handle_event(EventType, EventCount, Data) ->
  %% Ignore all other events.
  {keep_state, Data}.


clear_given_voted(Data0) ->
  Data0#?MODULE{given_voted=sets:new()}.

vote(NodeName, Data0) ->
  #?MODULE{given_voted=GivenVoted0} = Data0,
  GivenVoted = sets:add_element(NodeName, GivenVoted0),
  Data0#?MODULE{given_voted=GivenVoted}.

increase_current_term(Data0) ->
  #?MODULE{current_term=CurrentTerm} = Data0,
  Data0#?MODULE{current_term=CurrentTerm + 1}.

my_name() ->
  whereis(self()).

timeout_heartbeat(Pid) ->
  {ok, Timer} = timer:apply_after(?ELECTION_TIMEOUT, gen_statem, cast, [self(), append_entry_timeout]),
  Timer.

remove_append_entry_timer(Data0) ->
  Data0#?MODULE{append_entry_timer=undefined}.


