-module(raft_rpc_append_entries).

-include("rpc_record.hrl").

%% API
-export([commit_if_can/2]).
-export([get/2]).
-export([new/6]).
-export([new_ack_fail_with_default/2]).
-export([new_ack_fail/4]).
-export([new_ack_success/3]).
-export([find_earliest_index_with_same_term/3]).
-export([find_earliest_index_at_conflict_term/2]).
-export([find_last_index_with_same_term/2]).
-export([should_append_entries/3]).
-export([do_concat_entries/3]).
-export([do_append_entries/7]).
-export([concat_entries/3]).

-define(INVALID_MATCH_INDEX, -1).

new(Term, LeaderName, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit) ->
  #append_entries{term=Term,
                  leader_name = LeaderName,
                  previous_log_index = PrevLogIndex,
                  previous_log_term = PrevLogTerm,
                  entries = Entries,
                  leader_commit_index = LeaderCommit}.

new_ack_fail_with_default(NodeName, NodeTerm) ->
  AppendResult = #fail_append_entries{conflict_term=0,
                                      first_index_with_conflict_term=0},
  new_ack(NodeName, NodeTerm, false, AppendResult).

new_ack_fail(NodeName, NodeTerm, ConflictTerm, FirstIndexWithConflictTerm) ->
  AppendResult = #fail_append_entries{conflict_term=ConflictTerm,
                                      first_index_with_conflict_term=FirstIndexWithConflictTerm},
  new_ack(NodeName, NodeTerm, false, AppendResult).

new_ack_success(NodeName, NodeTerm, MatchIndex) ->
  AppendResult = #success_append_entries{match_index=MatchIndex},
  new_ack(NodeName, NodeTerm, true, AppendResult).

new_ack(NodeName, NodeTerm, Success, Result) ->
  #ack_append_entries{node_name=NodeName,
                      node_term=NodeTerm,
                      success=Success,
                      result=Result}.


get(term, #append_entries{term=Term}) ->
  Term;
get(leader_name, #append_entries{leader_name=LeaderName}) ->
  LeaderName;
get(previous_log_index, #append_entries{previous_log_index=PreviousLogIndex}) ->
  PreviousLogIndex;
get(previous_log_term, #append_entries{previous_log_term=PreviousLogTerm}) ->
  PreviousLogTerm;
get(entries, #append_entries{entries=Entries}) ->
  Entries;
get(leader_commit_index, #append_entries{leader_commit_index=LeaderCommitIndex}) ->
  LeaderCommitIndex;
get(_, _AppendEntries) ->
  undefined.

should_append_entries(PrevLogIndexFromLeader, PrevLogTermFromLeader, LogsFromMe) ->
  PrevTermFromMe = get_log_term(PrevLogIndexFromLeader, LogsFromMe),
  (PrevLogIndexFromLeader =:= 0 orelse
    (PrevLogIndexFromLeader =< length(LogsFromMe) andalso PrevLogTermFromLeader =:= PrevTermFromMe)
  ).

find_earliest_index_at_conflict_term(0, _LogsEntriesFromMe) ->
  error({invalid_state, "It cannot be reached at here. because the function `should_append_entries` already filtered it."});
find_earliest_index_at_conflict_term(PrevIndex, LogsEntriesFromMe) when PrevIndex > length(LogsEntriesFromMe) ->
  case LogsEntriesFromMe of
    [] -> {0, 0};
    LogsEntriesFromMe ->
      [Head|_Rest] = LogsEntriesFromMe,
      {ConflictTerm, _Data} = Head,
      FoundFirstIndexWithConflictTerm = find_earliest_index_with_same_term_(ConflictTerm, length(LogsEntriesFromMe), LogsEntriesFromMe, unknown, false),
      {ConflictTerm, FoundFirstIndexWithConflictTerm}
  end;
find_earliest_index_at_conflict_term(PrevIndex, LogsEntriesFromMe) ->
  ReversedLogs = lists:reverse(LogsEntriesFromMe),
  ReversedSubList = lists:sublist(ReversedLogs, PrevIndex),
  SubListFromMe = lists:reverse(ReversedSubList),

  [Head|_Rest] = SubListFromMe,
  {ConflictTerm, _Data} = Head,
  FoundFirstIndexWithConflictTerm = find_earliest_index_with_same_term_(ConflictTerm, PrevIndex, SubListFromMe, unknown, false),
  {ConflictTerm, FoundFirstIndexWithConflictTerm}.


find_last_index_with_same_term(_ConflictTerm, []) ->
  {false, ?INVALID_MATCH_INDEX};
find_last_index_with_same_term(ConflictTerm, LogEntries) ->
  find_last_index_with_same_term_(ConflictTerm, length(LogEntries), LogEntries, unknown, false).

find_last_index_with_same_term_(_ConflictTerm, Index, _LogEntries, done, true) ->
  {true, Index};
find_last_index_with_same_term_(_ConflictTerm, Index, [], _PreviousStateOfComparingResult, _End) ->
  {false, ?INVALID_MATCH_INDEX};
find_last_index_with_same_term_(ConflictTerm, Index, [Head|Rest], PreviousStateOfComparingResult, _End) ->
  {CurIndexTerm, _} = Head,
  CurrentComparingResult = raft_util:compare(CurIndexTerm, ConflictTerm),
  case {PreviousStateOfComparingResult, CurrentComparingResult} of
    {_, equal} -> find_last_index_with_same_term_(ConflictTerm, Index, [], done, true);
    {_, _} -> find_last_index_with_same_term_(ConflictTerm, Index-1, Rest, CurrentComparingResult, false)
  end.

find_earliest_index_with_same_term(_PrevTermFromLeader, PrevIndex, []) ->
  max(PrevIndex-1, 0);
find_earliest_index_with_same_term(PrevTermFromLeader, PrevIndex, LogEntries) ->
  find_earliest_index_with_same_term_(PrevTermFromLeader, PrevIndex, LogEntries, unknown, false).

find_earliest_index_with_same_term_(_PrevTermFromLeader, Index, _LogEntries, _PreviousStateOfComparingResult, true) ->
  Index;
find_earliest_index_with_same_term_(_PrevTermFromLeader, Index, [], _PreviousStateOfComparingResult, _End) ->
  Index;
find_earliest_index_with_same_term_(PrevTermFromLeader, Index, [Head|Rest], PreviousStateOfComparingResult, _End) ->
  {CurIndexTerm, _} = Head,
  CurrentComparingResult = raft_util:compare(CurIndexTerm, PrevTermFromLeader),
  case {PreviousStateOfComparingResult, CurrentComparingResult} of
    {equal, less} -> find_earliest_index_with_same_term_(PrevTermFromLeader, Index+1, [], done, true);
    {_, _} -> find_earliest_index_with_same_term_(PrevTermFromLeader, Index-1, Rest, CurrentComparingResult, false)
  end.

get_log_term(0, _Logs) ->
  -1;

get_log_term(Index, Logs) when Index > length(Logs) ->
  -1;

get_log_term(Index, Logs) when Index > 0 ->
  ReversedLogs = lists:reverse(Logs),
  {Term, _Entry} = lists:nth(Index, ReversedLogs),
  Term.

do_concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader) ->
  ReversedLogsIHave = lists:reverse(LogsIHave),
  ReversedLogsFromLeader = lists:reverse(LogsFromLeader),
  {UpdatedList, MatchedIndex} = concat_entries(ReversedLogsIHave, ReversedLogsFromLeader, PrevIndexFromLeader),
  {lists:reverse(UpdatedList), MatchedIndex}.

concat_entries(LogsIHave, LogsFromLeader, PrevIndexFromLeader) ->

  {PrevSubList0, NextSubList0} =
    case PrevIndexFromLeader of
      0 -> {[], LogsIHave};
      PrevIndexFromLeader -> {
                              lists:sublist(LogsIHave, 1, PrevIndexFromLeader),
                              lists:sublist(LogsIHave, PrevIndexFromLeader + 1, length(LogsIHave))
                             }
    end,

  ConcatList = PrevSubList0 ++ concat_entries_(NextSubList0, LogsFromLeader, [], false),
  {ConcatList, length(ConcatList)}.

concat_entries_([], [], Acc, _NotMatched) ->
  lists:reverse(Acc);

concat_entries_([], LogsFromLeader, Acc, false) ->
  lists:reverse(Acc) ++ LogsFromLeader;

concat_entries_(_, [], Acc, _NotMatched) ->
  lists:reverse(Acc);

concat_entries_(_, LogsFromLeader, Acc, true) ->
  lists:reverse(Acc) ++ LogsFromLeader;

concat_entries_([Head1|Tail1], [Head2|Tail2], Acc0, false) ->
  {TermFromMe, DataFromMe} = Head1,
  {TermFromLeader, DataFromLeader} = Head2,

  IsDifferent = TermFromMe =/= TermFromLeader orelse DataFromMe =/= DataFromLeader,
  concat_entries_(Tail1, Tail2, [Head2|Acc0], IsDifferent).


do_append_entries([], _MatchIndex, _LogEntries, _NextIndex, _CurrentTerm, _CommitIndex, RpcDueAcc) ->
  RpcDueAcc;
do_append_entries([Member|Rest], MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0) ->
  MatchIndexOfMember = maps:get(Member, MatchIndex),
  HasLagOfLog = MatchIndexOfMember < length(LogEntries),

  RpcDueOfMember = maps:get(Member, RpcDueAcc0),
  IsRpcExpired = raft_rpc_timer_utils:is_rpc_expired(RpcDueOfMember),

  case HasLagOfLog orelse IsRpcExpired of
    false -> do_append_entries(Rest, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc0);
    true ->
      NextRpcExpiredTime = raft_rpc_timer_utils:next_rpc_due_divide_by(2),
      RpcDueAcc = maps:put(Member, NextRpcExpiredTime, RpcDueAcc0),

      PrevIndex = max(maps:get(Member, NextIndex) - 1, 0),
      PrevTerm =
        case {LogEntries, PrevIndex} of
          {[], _} -> 0;
          {LogEntries, 0} -> 0;
          {LogEntries, PrevIndex} ->
            ReversedLogs0 = lists:reverse(LogEntries),
            {Term0, _} = lists:nth(PrevIndex, ReversedLogs0),
            Term0
        end,

      ToBeSentEntries = case {LogEntries, PrevIndex} of
                          {[], _} -> [];
                          {LogEntries, 0} -> LogEntries;
                          {LogEntries, PrevIndex} ->
                            ReversedLogs1 = lists:reverse(LogEntries),
                            lists:reverse(lists:sublist(ReversedLogs1, PrevIndex + 1, length(LogEntries)))
                        end,

      AppendEntriesRpc = raft_rpc_append_entries:new(CurrentTerm, my_name(), PrevIndex, PrevTerm, ToBeSentEntries, CommitIndex),
      AppendEntriesRpcMsg = {append_entries, AppendEntriesRpc},

      ToMemberPid = raft_util:get_node_pid(Member),
      gen_statem:cast(ToMemberPid, AppendEntriesRpcMsg),
      do_append_entries(Rest, MatchIndex, LogEntries, NextIndex, CurrentTerm, CommitIndex, RpcDueAcc)
  end.


commit_if_can(MatchIndex, TotalMemberSize) ->
  SortFunc = fun({MatchIndex1, _MatchCount1}, {MatchIndex2, _MatchCount2}) ->
                MatchIndex1 > MatchIndex2
             end,

  IndexAndAckedCountMap = maps:fold(fun(_, MatchIndex0, Acc0) ->
                                      Count0 = maps:get(MatchIndex0, Acc0, 0),
                                      maps:put(MatchIndex0, Count0+1, Acc0)
                                    end, #{}, MatchIndex),

  IndexAndAckedCountList = maps:to_list(IndexAndAckedCountMap),
  SortedMatchIndexDescOrderOfIndex = lists:sort(SortFunc, IndexAndAckedCountList),

  {_, MaybeNewCommitIndex} =
    lists:foldl(fun({Index, MatchCount}, {AckedMatchCount0, MaxMatchIndexAcc}) ->
                  AckedMatchCount = AckedMatchCount0 + MatchCount,
                  case raft_leader_election:has_quorum(TotalMemberSize, AckedMatchCount) of
                    true -> {AckedMatchCount, max(Index, MaxMatchIndexAcc)};
                    false -> {AckedMatchCount, MaxMatchIndexAcc}
                  end
                end, {0, 0}, SortedMatchIndexDescOrderOfIndex),
  MaybeNewCommitIndex.

my_name() ->
  raft_util:my_name().