-module(raft_rpc_request_vote).

-include("rpc_record.hrl").

-export([vote/3]).
-export([vote_my_self/2]).
-export([new_request_vote/4]).
-export([new_ack/3]).
-export([request_vote/2]).
-export([handle_request_vote_rpc/2]).

request_vote(NodeName, VoteArgs) when is_atom(NodeName) ->
  Pid = whereis(NodeName),
  % Should be cast. otherwise, deadlock occur.
  % (Candidate A wait ack_voted from B, B wait ack_voted_from A)
  gen_statem:cast(Pid, {request_vote, VoteArgs});
request_vote(Pid, VoteArgs) when is_pid(Pid)->
  gen_statem:cast(Pid, {request_vote, VoteArgs}).


new_request_vote(NodeName, NodeTerm, LastLogIndex, LastLogTerm) ->
  #vote_args{candidate_term=NodeTerm,
             candidate_name=NodeName,
             candidate_last_log_index=LastLogIndex,
             candidate_last_log_term=LastLogTerm}.

new_ack(CurrentTerm, VoteGranted, MyName) ->
  {ack_request_voted, MyName, CurrentTerm, VoteGranted}.

vote(CandidateName, NewTerm, RaftState0) ->
  #raft_state{vote_granted=VoteGranted0} = RaftState0,
  VoteGranted = sets:add_element(CandidateName, VoteGranted0),
  RaftState0#raft_state{vote_granted=VoteGranted, voted_for=CandidateName, current_term=NewTerm}.

vote_my_self(NewTerm, RaftState0) ->
  MyName = raft_util:node_name(self()),
  vote(MyName, NewTerm, RaftState0).

handle_request_vote_rpc(RaftState0, VoteArgs) ->
  #raft_state{voted_for=VotedFor,
    current_term=CurrentTerm,
    last_applied=FollowerLogLastIndex,
    last_log_term=FollowerLogLastTerm} = RaftState0,

  #vote_args{candidate_name=CandidateName} = VoteArgs,

  case raft_leader_election:can_vote(VotedFor, CurrentTerm, FollowerLogLastTerm, FollowerLogLastIndex, VoteArgs) of
    true ->
      RaftState1 = raft_scheduler:schedule_heartbeat_timeout_and_cancel_previous_one(RaftState0),
      RaftState2 = raft_rpc_request_vote:vote(CandidateName, CurrentTerm, RaftState1),
      ack_request_voted(CandidateName, CurrentTerm, true),
      {keep_state, RaftState2};
    false ->
      ack_request_voted(CandidateName, CurrentTerm, false),
      {keep_state, RaftState0}
  end.

ack_request_voted(CandidateName, CurrentTerm, VoteGranted) ->
  io:format("[~p] Node ~p send ack_request_voted to ~p~n", [self(), raft_util:my_name(), CandidateName]),
  ToPid = raft_util:get_node_pid(CandidateName),
  RequestVoteRpc = new_ack(CurrentTerm, VoteGranted, raft_util:my_name()),
  gen_statem:cast(ToPid, RequestVoteRpc).