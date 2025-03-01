-module(raft_scheduler).
-author("ansanghyeog").

-include("rpc_record.hrl").
%% API
-export([schedule_heartbeat_timeout/0]).
-export([schedule_heartbeat_timeout_and_cancel_previous_one/1]).
-export([schedule_append_entries/1]).


schedule_heartbeat_timeout() ->
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Timer.

schedule_heartbeat_timeout_and_cancel_previous_one(Data) ->
  #raft_state{election_timeout_timer=PreviousTimer} = Data,
  timer:cancel(PreviousTimer),
  {ok, Timer} = timer:apply_after(jitter_election_timeout(), gen_statem, cast, [self(), election_timeout]),
  Data#raft_state{election_timeout_timer=Timer}.

schedule_append_entries(Data0) ->
  NextScheduled = raft_util:get_election_timeout_divided_by(4),
  {ok, Timer} = timer:apply_after(NextScheduled, gen_statem, cast, [self(), do_append_entries]),
  Data0#raft_state{append_entries_timer=Timer}.

jitter_election_timeout() ->
  Jitter = 1.0 + rand:uniform(),
  FloatTimeout = raft_util:get_timer_time() * Jitter,
  trunc(FloatTimeout).