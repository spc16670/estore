-module(estore_utils).

-export([
  get_module/0
  ,get_module/1
  ,get_db_config/2
  ,get_config/1
  ,get_value/3
]).

-include("estore.hrl").

get_module(Db) ->
  list_to_atom(atom_to_list(?APP) ++ "_" ++ atom_to_list(Db)).
get_module() ->
  list_to_atom(atom_to_list(?APP) ++ "_" ++ atom_to_list(get_config(default_db))).

get_db_config(Db,Key) ->
  DbConfig = get_value(Db,get_config(dbs),[]),
  get_value(Key,DbConfig,undefined).

get_config(Key) ->
  {ok,Value} = application:get_env(?APP,Key),
  Value.

get_value(Key,PropList,Default) ->
  case lists:keyfind(Key,1,PropList) of
    {_,Value} -> Value;
    _ -> Default
  end.


