-module(raft_rpc_request_vote).

-include("rpc_record.hrl").
%% API
-export([new_request_vote/4]).
-export([new_ack/3]).

new_request_vote(NodeName, NodeTerm, LastLogIndex, LastLogTerm) ->
  #vote_args{candidate_term=NodeTerm,
             candidate_name=NodeName,
             candidate_last_log_index=LastLogIndex,
             candidate_last_log_term=LastLogTerm}.

new_ack(CurrentTerm, VoteGranted, MyName) ->
  {ack_request_voted, MyName, CurrentTerm, VoteGranted}.




