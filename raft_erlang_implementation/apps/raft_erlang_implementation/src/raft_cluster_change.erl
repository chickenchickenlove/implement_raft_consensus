-module(raft_cluster_change).

%% API
-include("rpc_record.hrl").
-export([handle_cluster_change_if_needed/3]).
-export([handle_cluster_change_immediately/2]).

-spec handle_cluster_change_if_needed(Entries, AmILeader, State) -> ok when
  Entries :: list(),
  AmILeader :: boolean(),
  State :: #raft_state{}.
handle_cluster_change_if_needed([], _AmILeader, State) ->
  State;
handle_cluster_change_if_needed([Head|Rest], AmILeader, State0) ->
  {_Term, Data} = Head,
  case Data of
    {new_membership, NewMembership, OldMembership} ->
      StateAfterPrepareChange = prepare_new_membership_(NewMembership, OldMembership, State0),
      handle_cluster_change_if_needed(Rest, AmILeader, StateAfterPrepareChange);
    {confirm_new_membership, NewMembership} ->
      case AmILeader of
        true ->
          #raft_state{members=Members} = State0,
          #members{old_members=OldMembers} = Members,
          OldMembership = sets:to_list(OldMembers),
          send_deprecated_msg_if_needed(OldMembership, NewMembership);
        false -> ok
      end,
      StateAfterConfirmChange = confirm_new_membership_(NewMembership, State0),
      handle_cluster_change_if_needed(Rest, AmILeader, StateAfterConfirmChange);
    _ ->
      handle_cluster_change_if_needed(Rest, AmILeader, State0)
  end.

handle_cluster_change_immediately(Entry, State0) ->
  {Term, Data} = Entry,
  case Data of
    {new_membership, NewMembership} ->
      State1 = prepare_new_membership_(NewMembership, State0),
      #raft_state{members=Members} = State1,
      #members{old_members=OldMembers} = Members,
      OldMembership = sets:to_list(OldMembers),
      NewData = {new_membership, NewMembership, OldMembership},
      {State1, {Term, NewData}};
    _ ->
      {State0, Entry}
  end.

prepare_new_membership_(NewMembership, State0) ->
  #raft_state{members=Members0} = State0,
  #members{new_members=NewMembers0} = Members0,
  OldMembers = NewMembers0,
  NewMembers = sets:from_list(NewMembership),

  Members = Members0#members{new_members=NewMembers, old_members=OldMembers},
  State0#raft_state{members=Members}.

prepare_new_membership_(NewMembership, OldMembership, State0) ->
  OldMembers = sets:from_list(OldMembership),
  NewMembers = sets:from_list(NewMembership),
  Members = #members{new_members=NewMembers, old_members=OldMembers},
  State0#raft_state{members=Members}.

confirm_new_membership_(NewMembership, State0) ->
  #raft_state{members=Members0, next_index=NextIndex0, match_index=MatchIndex0, rpc_due=RpcDue0} = State0,
  #members{old_members=OldMembers} = Members0,
  OldMembersList = sets:to_list(OldMembers),

  ShouldRemoveList = OldMembersList -- NewMembership,
  NextIndex = raft_util:safe_remove_key(ShouldRemoveList, NextIndex0),
  MatchIndex = raft_util:safe_remove_key(ShouldRemoveList, MatchIndex0),
  RpcDue = raft_util:safe_remove_key(ShouldRemoveList, RpcDue0),

  Members = Members0#members{old_members=sets:new()},
  State0#raft_state{members=Members, match_index=MatchIndex, next_index=NextIndex, rpc_due=RpcDue}.


send_deprecated_msg_if_needed(OldMembership, NewMembership) ->
  DeprecatedMembersList = OldMembership -- NewMembership,
  lists:foreach(
    fun(NodeName) ->
      gen_statem:cast(NodeName, deprecated)
    end, DeprecatedMembersList).