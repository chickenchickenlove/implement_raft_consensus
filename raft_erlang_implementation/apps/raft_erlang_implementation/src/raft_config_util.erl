-module(raft_config_util).

%% API
-export([get/2]).


get(Key, DefaultValue) ->
  try
    persistent_term:get(Key)
  catch
    _:_ -> DefaultValue
  end.


