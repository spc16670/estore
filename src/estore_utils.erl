-module(estore_utils).

-export([
  get_module/0
  ,get_module/1
  ,get_db_config/2
  ,get_config/1
  ,get_value/3
  ,remove_dups/1
  ,format_time/2
  ,format_date/2
  ,format_datetime/2
  ,format_iso8601/0
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

%% @private
remove_dups([]) -> 
  [];
remove_dups([H|T]) -> 
  [H | [X || X <- remove_dups(T), X /= H]].


format_time({Hour,Min,Sec},'iso8601') ->
  io_lib:format("~2.10.0B:~2.10.0B:~2.10.0B",[Hour, Min, Sec]).

format_date({Year,Month,Day},'iso8601') ->
  io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B",[Year, Month, Day]).

format_datetime({{Year,Month,Day},{Hour,Min,Sec}},'iso8601') ->
  Date = io:fwrite("~s\n",[io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B",
  [Year, Month, Day, Hour, Min, Sec])]),
  Date.
 
format_iso8601() ->
  {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
  iolist_to_binary(io_lib:format(
    "~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0wZ",
    [Year, Month, Day, Hour, Min, Sec] )).
