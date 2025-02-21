-module(raft_request_vote_rpc).

%% API
-export([new_request_vote/4]).
-export([new_ack/3]).

new_request_vote(NodeName, NodeTerm, LastLogIndex, LastLogTerm) ->
  {NodeTerm, NodeName, LastLogIndex, LastLogTerm}.

new_ack(CurrentTerm, VoteGranted, MyName) ->
  {ack_request_voted, MyName, CurrentTerm, VoteGranted}.




