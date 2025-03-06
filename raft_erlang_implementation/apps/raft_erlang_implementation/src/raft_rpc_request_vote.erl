-module(raft_rpc_request_vote).

-include("rpc_record.hrl").

-export([vote/3]).
-export([vote_granted/3]).
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
  #raft_state{vote_granted=VoteGranted0, members=Members} = RaftState0,
  #members{new_members=NewMembers, old_members=OldMembers} = Members,
  #vote_granted{new_members=NewMembersVoteGranted0, old_members=OldMembersVoteGranted0} = VoteGranted0,

  NewMembersVoteGranted =
    case sets:is_element(CandidateName, NewMembers) of
      true -> sets:add_element(CandidateName, NewMembersVoteGranted0);
      false -> NewMembersVoteGranted0
    end,

  OldMembersVoteGranted =
    case sets:is_element(CandidateName, OldMembers) of
      true -> sets:add_element(CandidateName, OldMembersVoteGranted0);
      false -> OldMembersVoteGranted0
    end,

  VoteGranted = #vote_granted{new_members=NewMembersVoteGranted, old_members=OldMembersVoteGranted},
  RaftState0#raft_state{vote_granted=VoteGranted, voted_for=CandidateName, current_term=NewTerm}.

vote_my_self(NewTerm, RaftState0) ->
  MyName = raft_util:node_name(self()),
  vote(MyName, NewTerm, RaftState0).


vote_granted(FromName, Members, VotedGranted0) ->
  #members{new_members=NewMembers, old_members=OldMembers} = Members,
  % new_members이면서, old_members인 경우가 있을 수도 있네?
  % old: A,B,C
  % new: A,B,D
  % new + old : A,B,C,D
  % new, old를 다 신경쓰는거지.
  % TODO: VoteGranted도 옮겨야 함.
  VotedGranted1 =
    case sets:is_element(FromName, NewMembers) of
      true ->
        #vote_granted{new_members=VoteGrantedNewMembers0} = VotedGranted0,
        VoteGrantedNewMembers1 = sets:add_element(FromName, VoteGrantedNewMembers0),
        VotedGranted0#vote_granted{new_members=VoteGrantedNewMembers1};
      false ->
        VotedGranted0
    end,

  VotedGranted2 =
    case sets:is_element(FromName, OldMembers) of
      true ->
        #vote_granted{old_members=VoteGrantedOldMembers0} = VotedGranted1,
        VoteGrantedOldMembers1 = sets:add_element(FromName, VoteGrantedOldMembers0),
        VotedGranted1#vote_granted{old_members=VoteGrantedOldMembers1};
      false ->
        VotedGranted1
    end,

  VotedGranted2.


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