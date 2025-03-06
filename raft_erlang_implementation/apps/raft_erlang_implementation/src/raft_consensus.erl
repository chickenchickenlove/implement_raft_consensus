-module(raft_consensus).

-include("rpc_record.hrl").

-export([has_quorum/2]).

has_quorum(TotalMemberSize, VotedMemberCount) ->
  VotedMemberCount >= (TotalMemberSize div 2) + 1.