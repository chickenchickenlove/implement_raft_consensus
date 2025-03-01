-module(raft_rpc_timer_utils).

%% API
-export([is_rpc_expired/1]).
-export([infinity_rpc_due/0]).
-export([next_rpc_due/0]).
-export([next_rpc_due_divide_by/1]).
-export([current_time/0]).

% unit : millisecond.
-define(RPC_TIMEOUT, 50).

is_rpc_expired(MemorizedTime) ->
  Now = current_time(),
  MemorizedTime < Now.

infinity_rpc_due() ->
  current_time() * 10.

next_rpc_due() ->
  current_time() + ?RPC_TIMEOUT.

next_rpc_due_divide_by(DivideNum) ->
  DividedRpcTimeout = ?RPC_TIMEOUT div DivideNum,
  current_time() + DividedRpcTimeout.

current_time() ->
  os:system_time(millisecond).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTION %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
