-module(raft_leader_election).

-include("rpc_record.hrl").
%% API
-export([new_vote_arguments/4]).
-export([can_vote/5]).
-export([has_quorum/2]).
-export([has_quorum1/2]).

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
  #vote_args{candidate_name=NodeName,
             candidate_term=NodeTerm,
             candidate_last_log_index=LastLogIndex,
             candidate_last_log_term=LastLogTerm}.

has_quorum1(Members, VotedFor) ->
  #members{new_members=NewMembers, old_members=OldMembers} = Members,
  #vote_granted{new_members=NewMembersVoted, old_members=OldMembersVoted} = VotedFor,

  OldMembersSize = sets:size(OldMembers),
  NewMembersSize = sets:size(NewMembers),

  case {NewMembersSize, OldMembersSize} of
    {NewMembersSize, OldMembersSize} when OldMembersSize =:= 0 ->
      has_quorum(NewMembersSize, sets:size(NewMembersVoted));
    {NewMembersSize, OldMembersSize} ->
      has_quorum(NewMembersSize, sets:size(NewMembersVoted)) andalso
      has_quorum(OldMembersSize, sets:size(OldMembersVoted))
  end.

has_quorum(TotalMemberSize, VotedMemberCount) ->
  VotedMemberCount >= (TotalMemberSize div 2) + 1.


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