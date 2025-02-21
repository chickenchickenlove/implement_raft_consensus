-module(raft_node_state_machine_flaky_test).

%% API
-export([]).
-include_lib("eunit/include/eunit.hrl").

%%%% TODO: There is no guarantee for specified node to become leader. So, this test code is invalid.
%%%%% NOT ACTUALLY Implemented.
%%%%% NODE A vote to itself, Node B also vote to A.
%%candidate_become_leader_eventually_after_split_vote_test() ->
%%  %%% GIVEN
%%  raft_util:set_timer_time(500),
%%  {ok, Pid1} = raft_node_state_machine:start('A', ['A', 'B', 'C']),
%%  {ok, Pid2} = raft_node_state_machine:start('B', ['A', 'B', 'C']),
%%
%%  %%% WHEN
%%  timer:sleep(650),
%%
%%  %%% THEN
%%  State = raft_node_state_machine:get_state(Pid1),
%%  CurrentTerm = raft_node_state_machine:get_current_term(Pid1),
%%  VotedCount = raft_node_state_machine:get_voted_count(Pid1),
%%
%%  ?assertEqual(leader, State),
%%  ?assertEqual(1, CurrentTerm),
%%  ?assertEqual(2, VotedCount),
%%
%%  raft_node_state_machine:stop('B'),
%%  raft_node_state_machine:stop('A').