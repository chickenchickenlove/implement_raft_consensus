-module(raft_leader_election).

-include("rpc_record.hrl").
%% API
-export([new_vote_arguments/4]).
-export([can_vote/5]).
-export([has_quorum/2]).

-export_type([vote_arguments/0]).

% Candidate's Term.
-type candidate_term() :: integer().

% Candidate requesting vote.
-type candidate_id() :: atom().

% Index of candidate's last log entry
-type last_log_index() :: integer().

% Term of candidate's last log entry
-type last_log_term() :: integer().

-type vote_arguments() ::
  {candidate_term(), candidate_id(), last_log_index(), last_log_term()}.

new_vote_arguments(NodeName, NodeTerm, LastLogIndex, LastLogTerm) ->
  {NodeTerm, NodeName, LastLogIndex, LastLogTerm}.

start_election(Members, VoteArgs) ->
  FilteredMembers = sets:filter(
    fun(MemberName) ->
      MemberPid = whereis(MemberName),
      MemberPid =/= self()
    end, Members),

  lists:foreach(
    fun(MemberName) ->
      MemberPid = whereis(MemberName),
      gen_server:cast(MemberPid, VoteArgs)
    end, sets:to_list(FilteredMembers)).

has_quorum(TotalMemberSize, VotedMemberCount) ->
  VotedMemberCount * 2 > TotalMemberSize.
%%  VotedMemberCount >= ((TotalMemberSize div 2) + (TotalMemberSize rem 2)).

%% 리더 기반 합의 알고리즘에서는 리더가 모든 커밋된 로그를 포함해야 한다.
% Raft는 리더가 선출될 때부터 모든 커밋된 로그를 보장하도록 설계됨.
% 이를 통해 로그가 리더에서 팔로워로만 흐르게 유지할 수 있음.
% 즉, 리더는 기존의 로그를 덮어쓰지 않는다.

can_vote(VotedFor, FollowerTerm, FollowerLastLogTerm, FollowerLastLogIndex, VotedArgs) ->
  #vote_args{candidate_name=CandidateName,
             candidate_term=CandidateTerm,
             candidate_last_log_term=CandidateLastLogTerm,
             candidate_last_log_index=CandidateLastLogIndex} = VotedArgs,

  IsEqualTerm = FollowerTerm =:= CandidateTerm,
  NotYetVotedOrVotedSamePeer = VotedFor =:= undefined orelse VotedFor =:= CandidateName,
  CandidateHasLatestLogTerm = ((CandidateLastLogTerm > FollowerLastLogTerm) orelse
    (CandidateLastLogTerm =:= FollowerLastLogTerm andalso CandidateLastLogIndex >= FollowerLastLogIndex)
  ),
  IsEqualTerm andalso NotYetVotedOrVotedSamePeer andalso CandidateHasLatestLogTerm.