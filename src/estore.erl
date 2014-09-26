-module(estore).

-export([
  start/0
]).

-export([
  init/0
  ,models/0
  ,new/1
  ,save/1
  ,delete/2
  ,find/2
]).


-export([
  init/1
  ,models/1
  ,new/2
  ,save/2
  ,delete/3
  ,find/3
]).

-behaviour(estore_interface).

-include("estore.hrl").

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

delete(Record,Conditions) ->
  delete(estore_utils:get_module(),Record,Conditions).
delete(Module,Record,Conditions) ->
  M = estore_utils:get_module(Module),
  M:delete(Record,Conditions).

find(Name,Conditions) ->
  find(estore_utils:get_module(),Name,Conditions).
find(Module,Name,Conditions) ->
  M = estore_utils:get_module(Module),
  M:find(Name,Conditions).

