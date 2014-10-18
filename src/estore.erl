-module(estore).

-export([
  start/0
]).

-export([
  init/0
  ,models/0
  ,new/1
  ,save/1
  ,delete/1
  ,delete/2
  ,find/2
  ,find/5
]).


-export([
  init/1
  ,models/1
  ,new/2
  ,save/2
  ,delete/3
  ,find/3
  ,find/6
]).

-export([
  json_to_record/1
  ,record_to_json/1
]).

-behaviour(estore_interface).

-include("$RECORDS_PATH/estore.hrl").

%% -----------------------------------------------------------------------------
start() -> 
  start(?APP).

start(App) ->
  application:start(App,permanent).

init() ->
  init(estore_utils:get_module()).
init(Module) ->
  M = estore_utils:get_module(Module),
  M:init().

models() ->
  models(estore_utils:get_module()).
models(Module) ->
  M = estore_utils:get_module(Module),
  M:models().

new(Name) ->
  new(estore_utils:get_module(),Name).
new(Module,Name) ->
  M = estore_utils:get_module(Module),
  M:new(Name).

save(Record) ->
  save(estore_utils:get_module(),Record).
save(Module,Record) ->
  M = estore_utils:get_module(Module),
  M:save(Record).

delete(Record) ->
  delete(estore_utils:get_module(),Record).
delete(Module,Record) when is_atom(Module), is_tuple(Record) ->
  M = estore_utils:get_module(Module),
  M:delete(Record);
delete(Name,Conditions) when is_atom(Name), is_list(Conditions) ->
  delete(estore_utils:get_module(),Name,Conditions).
delete(Module,Name,Conditions) ->
  M = estore_utils:get_module(Module),
  M:delete(Name,Conditions).

find(Name,Conditions) ->
  find(estore_utils:get_module(),Name,Conditions).
find(Module,Name,Conditions) ->
  M = estore_utils:get_module(Module),
  M:find(Name,Conditions).

find(Name,Where,OrderBy,Limit,Offset) ->
  find(estore_utils:get_module(),Name,Where,OrderBy,Limit,Offset).
find(Module,Name,Where,OrderBy,Limit,Offset) ->
  M = estore_utils:get_module(Module),
  M:find(Name,Where,OrderBy,Limit,Offset).

%% ----------------------------------------------------------------------------

json_to_record(Json) ->
  estore_json:json_to_record(Json).

record_to_json(Record) -> 
  estore_json:record_to_json(Record).
