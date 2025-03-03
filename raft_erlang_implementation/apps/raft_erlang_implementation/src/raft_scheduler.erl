-module(raft_scheduler).
-author("ansanghyeog").

-include("rpc_record.hrl").
%% API
-export([schedule_heartbeat_timeout/0]).
-export([schedule_heartbeat_timeout_and_cancel_previous_one/1]).
-export([schedule_append_entries/1]).


schedule_heartbeat_timeout() ->
  GivenTime = raft_util:get_timer_time(),
  ExpiredTime = jitter_election_timeout(GivenTime),
  {ok, Timer} = timer:apply_after(ExpiredTime, gen_statem, cast, [self(), election_timeout]),
  Timer.

schedule_heartbeat_timeout_and_cancel_previous_one(Data) ->
  #raft_state{election_timeout_timer=PreviousTimer} = Data,
  timer:cancel(PreviousTimer),

  GivenTime = raft_util:get_timer_time(),
  ExpiredTime = jitter_election_timeout(GivenTime),

  {ok, Timer} = timer:apply_after(ExpiredTime, gen_statem, cast, [self(), election_timeout]),
  Data#raft_state{election_timeout_timer=Timer}.

schedule_append_entries(Data0) ->
  ElectionTimeout = raft_util:get_timer_time(),
  DividedNumber = 4,
  ExpiredAfter = ElectionTimeout div DividedNumber,

  {ok, Timer} = timer:apply_after(ExpiredAfter, gen_statem, cast, [self(), do_append_entries]),
  Data0#raft_state{append_entries_timer=Timer}.

jitter_election_timeout(GivenTime) ->
  Jitter = 1.0 + rand:uniform(),
  FloatTimeout = GivenTime * Jitter,
  trunc(FloatTimeout).