-module(model).

-export([
  create/0
  ,create/1
  ,db_type/0
  ,tablespace/0
]).

-include("models.hrl").

%% -----------------------------------------------------------------------------

db_type() ->
  pgsql.

tablespace() ->
  lamazone.

create() ->
  create(#models{}).

create(Model) ->
  Map = Model#models.address,
  io:fwrite("~p~n",[maps:keys(Map)]),
  io:fwrite("~p~n",[db_type()]),
  io:fwrite("~p~n",[tablespace()]).
