-module(raft_leader_election_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").


win_test() ->
  Result1 = raft_leader_election:has_quorum(5, 3),
  ?assertEqual(true, Result1),
  Result2 = raft_leader_election:has_quorum(5, 2),
  ?assertEqual(false, Result2),
  Result3 = raft_leader_election:has_quorum(4, 2),
  ?assertEqual(false, Result3),
  Result4 = raft_leader_election:has_quorum(4, 3),
  ?assertEqual(true, Result4).




