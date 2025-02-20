-module(raft_request_vote_rpc).

%% API
-export([new/3]).

new(CurrentTerm, VoteGranted, MyName) ->
  {ack_request_voted, MyName, CurrentTerm, VoteGranted}.




