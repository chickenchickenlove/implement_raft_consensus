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
  #vote_args{candidate_name=NodeName,
             candidate_term=NodeTerm,
             candidate_last_log_index=LastLogIndex,
             candidate_last_log_term=LastLogTerm}.

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