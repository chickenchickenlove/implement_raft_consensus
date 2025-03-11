-module(raft_test_factory).

-include("rpc_record.hrl").

-export([default_raft_config/0]).
-export([raft_config/1]).

default_raft_config() ->
  #raft_configuration{}.

raft_config(Configs) ->
  raft_config_(Configs, default_raft_config()).

raft_config_([], Acc) ->
  Acc;
raft_config_([{max_log_size, MaxLogSize}|Rest], Acc0) ->
  raft_config_(Rest, Acc0#raft_configuration{max_log_size=MaxLogSize});
raft_config_([{snapshot_module, SnapshotModule}|Rest], Acc0) ->
  raft_config_(Rest, Acc0#raft_configuration{snapshot_module=SnapshotModule});
raft_config_([{interval_of_install_snapshot, IntervalOfInstallSnapshot}|Rest], Acc0) ->
  raft_config_(Rest, Acc0#raft_configuration{interval_of_install_snapshot=IntervalOfInstallSnapshot}).




