-module(raft_util).

-include("rpc_record.hrl").
%% API
-export([set_timer_time/1, get_timer_time/0, clean_up_timer_time/0]).
-export([get_node_pid/1]).
-export([members_except_me/1]).
-export([my_name/0]).
-export([node_name/1]).
-export([compare/2]).
-export([get_entry/3]).
-export([all_members/1]).
-export([safe_remove_key/2]).
-export([send_msg/2]).

send_msg(PeerName, Message) ->
  ToMemberPid = raft_util:get_node_pid(PeerName),
  gen_statem:cast(ToMemberPid, Message).

clean_up_timer_time() ->
  catch persistent_term:erase(election_timeout).

set_timer_time(Timeout) ->
  persistent_term:put(election_timeout, Timeout).

get_timer_time() ->
  try
    persistent_term:get(election_timeout)
  catch _:_ ->
    150
  end.

get_node_pid(NodeName) ->
  whereis(NodeName).

my_name() ->
  node_name(self()).

node_name(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.

compare(A, B) ->
  if
    A > B -> greater;
    A =:= B -> equal;
    A < B -> less
  end.

safe_remove_key([], Map) ->
  Map;
safe_remove_key([Key|Rest], Map0) ->
  Map =
    try
        maps:remove(Key, Map0)
    catch
       _:_  -> Map0
    end,
  safe_remove_key(Rest, Map).



% Assume that [1,2,3,4,5,6, ....]
get_entry(0, _End, []) ->
  [];

get_entry(0, End, Entries) ->
  lists:sublist(Entries, End);

get_entry(_Start, 0, _Entries) ->
  [];

get_entry(_Start, _End, []) ->
  [];

get_entry(Start, End, _Entries) when Start =:= End ->
  [];

get_entry(Start, End, Entries) ->
  lists:sublist(Entries, Start+1, End).

-spec members_except_me(Members) -> list(atom()) when
  Members :: #raft_state{}.
members_except_me(Members) ->
  MyName = my_name(),
  #members{new_members=NewMembers0, old_members=OldMembers0} = Members,
  UnionMembers = sets:union(NewMembers0, OldMembers0),
  sets:to_list(sets:del_element(MyName, UnionMembers)).


all_members(Members) ->
  #members{new_members=NewMembers0, old_members=OldMembers0} = Members,
  sets:union(NewMembers0, OldMembers0).
