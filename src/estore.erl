-module(estore).

-export([
  init/0
  ,models/0
  ,new/1
  ,save/1
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

-behaviour(estore_interface).

-include("estore.hrl").
-include("pgsql.hrl").

%% -----------------------------------------------------------------------------

init() ->
  init(estore_utils:get_module()).
init(Module) ->
  Module:init().

models() ->
  models(estore_utils:get_module()).
models(Module) ->
  Module:models().

new(Name) ->
  new(estore_utils:get_module(),Name).
new(Module,Name) ->
  Module:new(Name).

save(Record) ->
  save(estore_utils:get_module(),Record).
save(Module,Record) ->
  Module:save(Record).

delete(Record,Conditions) ->
  delete(estore_utils:get_module(),Record,Conditions).
delete(Module,Record,Conditions) ->
  Module:delete(Record,Conditions).

find(Name,Conditions) ->
  find(estore_utils:get_module(),Name,Conditions).
find(Module,Name,Conditions) ->
  Module:find(Name,Conditions).

find(Name,Where,OrderBy,Limit,Offset) ->
  find(estore_utils:get_module(),Name,Where,OrderBy,Limit,Offset).
find(Module,Name,Where,OrderBy,Limit,Offset) ->
  Module:find(Name,Where,OrderBy,Limit,Offset).

