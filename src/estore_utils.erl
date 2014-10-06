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
  ,bin_to_num/1
  ,root_dir/0
  ,enabled_dbs/0
  ,record_origin/1
  ,record_name/1
  ,record_names/1
  ,value_to_string/1
  ,is_proplist/1
]).

-include("$RECORDS_PATH/estore.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

root_dir() ->
  {ok,Path} = file:get_cwd(),
  Path.

get_module(Db) ->
  Prefix = atom_to_list(?APP) ++ "_",
  case string:str(atom_to_list(Db),Prefix) of
    0 -> list_to_atom(Prefix ++ atom_to_list(Db));
    1 -> Db;
    _ -> unknown_module
  end.

get_module() ->
  M = case get_config(default_db) of
    undefined -> {Db,_} = lists:nth(1,get_config(dbs)), Db;
    Module -> Module
  end,
  list_to_atom(atom_to_list(?APP) ++ "_" ++ atom_to_list(M)).

get_db_config(Db,Key) ->
  DbConfig = get_value(Db,get_config(dbs),[]),
  get_value(Key,DbConfig,undefined).

get_config(Key) ->
  case application:get_env(?APP,Key) of
    {ok,Value} -> Value;
    undefined -> undefined 
  end.

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

bin_to_num(Bin) ->
  N = binary_to_list(Bin),
  case string:to_float(N) of
    {error,no_float} -> list_to_integer(N);
    {F,_Rest} -> F
  end.

enabled_dbs() ->
  lists:foldl(fun({Name,_Conf},Acc) -> 
    Acc ++ [Name] 
  end, [], estore_utils:get_config(dbs)).

record_origin(Name) ->
  record_origin(Name,enabled_dbs()).

record_origin(Name,[Db|Dbs]) ->
  Module = get_module(Db),
  case lists:member(Name,record_names(Module:models())) of
    true -> Db;
    false -> record_origin(Name,Dbs)
  end;
record_origin(_Name,[]) ->
  undefined.
  
record_names(Records) ->
  lists:foldl(fun(Record,Acc) ->
    Acc ++ [record_name(Record)]
  end, [], Records).

record_name(Record) ->
  hd(tuple_to_list(Record)).

is_proplist([]) -> 
  true;
is_proplist([{_,_}|L]) -> 
  is_proplist(L);
is_proplist(_) -> 
  false.

%% ----------------------------------------------------------------------------

value_to_string(V) when is_atom(V) andalso V /= undefined ->
  atom_to_list(V);
value_to_string({V,Dec}) when is_float(V) ->
  float_to_list(V,Dec);
value_to_string(V) when is_integer(V) ->
  integer_to_list(V);
value_to_string(V) when is_list(V) ->
  V;
value_to_string(V) when is_binary(V) ->
  binary_to_list(V);
value_to_string(_V) ->
  [].

