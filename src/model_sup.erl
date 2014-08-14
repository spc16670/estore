-module(model_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  Pools = model:get_db_config(pgsql,pools),
  io:fwrite("~p~n",[Pools]),
  PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
    PoolArgs = [{name, {local, Name}},
      {worker_module, pgsql_worker}] ++ SizeArgs,
      poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
  io:fwrite("~p~n",[PoolSpecs]),
  {ok,{{one_for_one, 5,10},PoolSpecs}}.

