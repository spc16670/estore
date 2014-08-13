-module(model).

-export([
  get_db_config/2
  ,get_config/1
  ,get_module/0
  ,get_value/3
]).

-behaviour(model_interface).

-export([
  init/0
  ,models/0
  ,new/1
  ,save/1
  ,delete/1
  ,find/2
]).


-export([
  init/1
  ,models/1
  ,new/2
  ,save/2
  ,delete/2
  ,find/3
]).

-include("models_pgsql.hrl").

%% -----------------------------------------------------------------------------

init() ->
  init(get_module()).
init(Module) ->
  Module:init().

models() ->
  models(get_module()).
models(Module) ->
  Module:models().

new(Name) ->
  new(get_module(),Name).
new(Module,Name) ->
  Module:new(Name).

save(Record) ->
  save(get_module(),Record).
save(Module,Record) ->
  Module:save(Record).

delete(Record) ->
  delete(get_module(),Record).
delete(Module,Record) ->
  Module:save(Record).

find(Name,Conditions) ->
  find(get_module(),Name,Conditions).
find(Module,Name,Conditions) ->
  Module:find(Name,Conditions).

%% -----------------------------------------------------------------------------

get_module() ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(get_config(default_adapter))).

get_db_config(Adapter,Key) ->
  {ok,Config} = application:get_env(model,adapters),
  case lists:keyfind(Adapter,1,Config) of
    {_,AdapterConfig} -> 
      get_value(Key,AdapterConfig,undefined);
    _ -> undefined
  end.

get_config(Key) ->
  {ok,Config} = application:get_env(model,Key),
  case lists:keyfind(Key,1,Config) of
    {_,Value} -> Value;
    _ -> undefined
  end.

get_value(Key,PropList,Default) ->
  case lists:keyfind(Key,1,PropList) of
    {_,Value} -> Value;
    _ -> Default
  end.

