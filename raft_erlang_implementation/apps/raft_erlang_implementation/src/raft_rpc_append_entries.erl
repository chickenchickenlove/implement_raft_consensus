-module(raft_rpc_append_entries).

-include("rpc_record.hrl").

%% API
-export([get/2]).
-export([new/6]).
-export([new_ack/4]).
-export([new_ack_fail/2]).
-export([should_append_entries/3]).
-export([do_concat_entries/3]).
-export([concat_entries/3]).

-define(INVALID_MATCH_INDEX, -1).

new(Term, LeaderName, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit) ->
  #append_entries{term=Term,
                  leader_name = LeaderName,
                  previous_log_index = PrevLogIndex,
                  previous_log_term = PrevLogTerm,
                  entries = Entries,
                  leader_commit_index = LeaderCommit}.

new_ack_fail(NodeName, NodeTerm) ->
  % -1 is invalid value.
  % So, Caller should ignore this value if caller witness this.
  new_ack(NodeName, NodeTerm, false, ?INVALID_MATCH_INDEX).


new_ack(NodeName, NodeTerm, Success, MatchIndex) ->
  #ack_append_entries{node_name=NodeName,
                      node_term=NodeTerm,
                      success=Success,
                      match_index=MatchIndex}.


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
