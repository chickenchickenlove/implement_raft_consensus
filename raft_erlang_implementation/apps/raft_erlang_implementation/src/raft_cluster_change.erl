-module(raft_cluster_change).

%% API
-include("rpc_record.hrl").
-export([handle_cluster_change_if_needed/2]).
%%-export([handle_cluster_change_entry_from_client/2]).

-spec handle_cluster_change_if_needed(Entries, State) -> ok when
  Entries :: list(),
  State :: #raft_state{}.
handle_cluster_change_if_needed([], State) ->
  State;
handle_cluster_change_if_needed([Head|Rest], State0) ->
  {_Term, Data} = Head,
  case Data of
    {remove_members, RemovedMembers} ->
      % deprecated
      StateAfterRemoving = remove_members_(RemovedMembers, State0),
      handle_cluster_change_if_needed(Rest, StateAfterRemoving);
    {add_members, AddMembers} ->
      % deprecated
      StateAfterAdding = add_members_(AddMembers, State0),
      handle_cluster_change_if_needed(Rest, StateAfterAdding);
    {new_membership, NewMembership} ->
      StateAfterPrepareChange = prepare_new_membership_(NewMembership, State0),
      handle_cluster_change_if_needed(Rest, StateAfterPrepareChange);
    {confirm_new_membership, NewMembership} ->
      StateAfterConfirmChange = confirm_new_membership_(NewMembership, State0),
      handle_cluster_change_if_needed(Rest, StateAfterConfirmChange);
    _ ->
      handle_cluster_change_if_needed(Rest, State0)
  end.


prepare_new_membership_(NewMembership, State0) ->
  #raft_state{members=Members0} = State0,
  #members{new_members=NewMembers0} = Members0,
  OldMembers = NewMembers0,
  NewMembers = sets:from_list(NewMembership),

  Members = Members0#members{new_members=NewMembers, old_members=OldMembers},
  State0#raft_state{members=Members}.

confirm_new_membership_(_NewMembership, State0) ->
  #raft_state{members=Members0} = State0,
  Members = Members0#members{old_members=sets:new()},
  State0#raft_state{members=Members}.

add_members_([], State) ->
  State;
add_members_([Member|Rest], State0) ->
  #raft_state{members=Members0} = State0,
  Members = sets:add_element(Member, Members0),
  add_members_(Rest, State0#raft_state{members=Members}).

remove_members_([], State) ->
  State;
remove_members_([Member|Rest], State0) ->
  #raft_state{members=Members0} = State0,
  MyName = raft_util:my_name(),
  case MyName =:= Member of
    true -> remove_members_([], State0#raft_state{members=sets:new()});
    false ->
      UpdatedMembers = sets:del_element(Member, Members0),
      remove_members_(Rest, State0#raft_state{members=UpdatedMembers})
  end.