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
  ,date_to_erlang/2
  ,time_to_erlang/2
  ,datetime_to_erlang/2
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


format_time({_,{Hour,Min,Sec}},'iso8601') ->
  lists:flatten(io_lib:format("~2..0B:~2..0B:~2..0B", [Hour,Min,Sec])).

format_date({{Year,Month,Day},_},'iso8601') ->
  lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B", [Year,Month,Day])).

format_datetime(DateTime,Format) ->
  Date = format_date(DateTime,Format),
  Time = format_time(DateTime,Format),
  Date ++ " " ++ Time.

date_to_erlang(Date,'iso8601') ->
  [Y,M,D] = re:split(Date,"-",[{return,list}]),
  {list_to_integer(Y),list_to_integer(M),list_to_integer(D)}.
   
time_to_erlang(Time,'iso8601') ->
  [H,M,S] = re:split(Time,":",[{return,list}]),
  {list_to_integer(H),list_to_integer(M),list_to_integer(S)}.

datetime_to_erlang(DateTimeBin,Format) ->
  [Date,Time] = re:split(DateTimeBin," ",[{return,list}]),
  {date_to_erlang(Date,Format),time_to_erlang(Time,Format)}. 


 
