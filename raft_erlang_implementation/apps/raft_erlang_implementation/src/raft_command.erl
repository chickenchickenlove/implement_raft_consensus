-module(raft_command).


-export([set/2]).
-export([delete/1]).
-export([commands/2]).

set(Key, Value) ->
  {raft_set, Key, Value}.

delete(Key) ->
  {raft_delete, Key}.

commands([], LocalRaftState0) ->
  LocalRaftState0;
commands([FirstCommand|Rest]=_Commands, LocalRaftState0) ->
  LocalRaftState1 =
    case FirstCommand of
      {_Term, Command} -> command(Command, LocalRaftState0);
      FirstCommand -> LocalRaftState0
    end,
  commands(Rest, LocalRaftState1).

%% TODO: declare spec.
%% @private
command({raft_set, Key, Value}, LocalRaftState0) ->
  maps:put(Key, Value, LocalRaftState0);
command({raft_delete, Key}, LocalRaftState0) ->
  try
      maps:remove(Key, LocalRaftState0)
  catch
      _:_  ->  LocalRaftState0
  end;
command(_UnSupportedCommand, LocalRaftState) ->
  LocalRaftState.