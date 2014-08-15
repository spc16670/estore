-module(estore_utils).

-export([
  get_module/2
  ,get_config/1
  ,get_module/0
  ,get_value/3
]).

-include("estore.hrl").

get_module() ->
  list_to_atom(atom_to_list(?APP) ++ "_" ++ atom_to_list(get_config(adapter))).

get_config(Key) ->
  {ok,Config} = application:get_env(?APP,Key),
  case lists:keyfind(Key,1,Config) of
    {_,Value} -> Value;
    _ -> undefined
  end.

get_value(Key,PropList,Default) ->
  case lists:keyfind(Key,1,PropList) of
    {_,Value} -> Value;
    _ -> Default
  end.
