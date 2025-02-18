-module(raft_leader_election).

%% API
-export([new_vote_arguments/4]).
-export([can_vote/5]).
-export([is_win/2]).

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

is_win(TotalMemberSize, VotedMemberCount) ->
  io:format("TotalMemberSize: ~p, VotedMemberCount: ~p~n", [TotalMemberSize, VotedMemberCount]),
  VotedMemberCount >= ((TotalMemberSize div 2) + (TotalMemberSize rem 2)).

can_vote(VotedFor, KnownLastTerm, KnownLastLogIndex, VotedFor, VoteArgs) ->
  {CandidateTerm, CandidateName, CandidateLastLogIndex, CandidateLastLogTerm} = VoteArgs,

  case VotedFor of
    undefined ->
      CandidateTerm > CandidateLastLogIndex

    ;
    AlreadyVoted -> ok
  end.
