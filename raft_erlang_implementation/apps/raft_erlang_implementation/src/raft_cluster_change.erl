-module(raft_cluster_change).

%% API
-include("rpc_record.hrl").
-export([handle_cluster_change_if_needed/2]).

-spec handle_cluster_change_if_needed(Entries, State) -> ok when
  Entries :: list(),
  State :: #raft_state{}.

handle_cluster_change_if_needed([], State) ->
  State;
handle_cluster_change_if_needed([Head|Rest], State0) ->
  {_Term, Data} = Head,
  case Data of
    {remove_members, RemovedMembers} ->
      StateAfterRemoving = remove_members_(RemovedMembers, State0),
      handle_cluster_change_if_needed(Rest, StateAfterRemoving);
    {add_members, AddMembers} ->
      StateAfterAdding = add_members_(AddMembers, State0),
      handle_cluster_change_if_needed(Rest, StateAfterAdding)
  end.


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