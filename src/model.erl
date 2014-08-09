-module(model).

-export([
  create/0
  ,create/1
  ,db_type/0
  ,tablespace/0
]).

-behaviour(model_interface).

-export([
  save/2
  ,delete/2
  ,find/2
]).


%% -----------------------------------------------------------------------------

save(Name,Map) ->
  save(Name,Map,get_module()).
save(Name,Map,Module) ->
  Module:save(Name,Map).

delete(_Name,_Map) ->
  ok.

find(_Name,_Map) ->
  ok.

%% -----------------------------------------------------------------------------




%% -----------------------------------------------------------------------------

db_type() ->
  {ok,Type} = application:get_env(model,db_type),
  Type.

tablespace() ->
  {ok,Tablespace} = application:get_env(model,tablespace),
  Tablespace.

get_module() ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(db_type())).

models() ->
  {ok,Models} = application:get_env(model,models),
  Models.

create() ->
  create(models()).

create(Models) ->
  lists:foldl(fun(E,Acc) ->
    io:fwrite("~p~n",[E])
  end,[],Models),
  io:fwrite("~p~n",[db_type()]),
  io:fwrite("~p~n",[tablespace()]).
