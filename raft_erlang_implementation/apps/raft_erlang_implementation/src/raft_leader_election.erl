-module(raft_leader_election).

%% API
-export([new_vote_arguments/4]).
-export([can_vote/4]).
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

%% 리더 기반 합의 알고리즘에서는 리더가 모든 커밋된 로그를 포함해야 한다.
% Raft는 리더가 선출될 때부터 모든 커밋된 로그를 보장하도록 설계됨.
% 이를 통해 로그가 리더에서 팔로워로만 흐르게 유지할 수 있음.
% 즉, 리더는 기존의 로그를 덮어쓰지 않는다.

%%% Raft 논문(Section 5.4) 기준으로, 팔로워가 후보에게 투표할 수 있는 조건 중 핵심은 후보의 로그가 팔로워의 로그보다 ‘최신(up-to-date)’이어야 함.
can_vote(FollowerVotedFor, FollowerLastLogTerm, FollowerLastLogIndex, VoteArgs) ->
  {_CandidateTerm, _CandidateName, CandidateLastLogIndex, CandidateLastLogTerm} = VoteArgs,
  case FollowerVotedFor of
    undefined -> can_vote_(FollowerLastLogTerm, CandidateLastLogTerm, FollowerLastLogIndex, CandidateLastLogIndex);
    _AlreadyVoted -> false
  end.

can_vote_(KnownLastLogTerm, CandidateLastLogTerm, KnownLastIndex, CandidateLastIndex) ->
  if
    (KnownLastLogTerm =:= CandidateLastLogTerm) andalso (KnownLastIndex =< CandidateLastIndex) -> true;
    (KnownLastLogTerm < CandidateLastLogTerm) -> true;
    true -> false
  end.