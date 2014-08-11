-module(model).

-export([
  db_type/0
  ,tablespace/0
  ,get_module/0
]).

-behaviour(model_interface).

-export([
  init/0
  ,models/0
  ,make/1
  ,save/1
  ,delete/1
  ,find/2
]).

-include("models.hrl").

%% -----------------------------------------------------------------------------

init() ->
  init(get_module()).
init(Module) ->
  Module:init().

models() ->
  models(get_module()).
models(Module) ->
  Module:models().

make(Name) ->
  make(Name,get_module()).
make(Name,Module) ->
  Module:make(Name).

save(Record) ->
  save(Record,get_module()).
save(Record,Module) ->
  Module:save(Record).

delete(_Name) ->
  ok.

find(_Name,_Conditions) ->
  ok.

%% -----------------------------------------------------------------------------

db_type() ->
  {ok,Type} = application:get_env(model,db_type),
  Type.

tablespace() ->
  {ok,Tablespace} = application:get_env(model,tablespace),
  Tablespace.

get_module() ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(db_type())).

