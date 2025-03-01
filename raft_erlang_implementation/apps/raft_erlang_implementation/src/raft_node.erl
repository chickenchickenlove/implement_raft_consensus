-module(raft_node).

%% API
-export([]).

-record(raft, {
  raft_stat_process :: pid()
}).

-behavior(gen_server).