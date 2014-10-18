-module(estore_mnesia).

-export([
  init/0
  ,new/1
  ,models/0
  ,save/1
  ,delete/1
  ,delete/2
  ,find/2
  ,find/5
]).

-export([
  create_tables/1
  ,save_record/1
  ,convert_to_result/2
]).

-include("$RECORDS_PATH/mnesia.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

new(Name) ->
 new_model(Name).

save(Model) ->
  save_record(Model).

delete(Record) ->
  Name = estore_utils:record_name(Record),
  Id = get_value(id, Record),
  delete(Name,Id).

delete(Name,Id) ->
  remove(Name,Id).

find(Name,Conditions) when is_list(Conditions) ->
  IsKey = io_lib:printable_list(Conditions),
  find(Name,Conditions,IsKey);
find(Name,Conditions) ->
  find(Name,Conditions,true).

find(Name,Id,true) -> 
  match(Name,Id);
find(Name,Where,false) -> 
  select(Name,Where,[],50,0).

find(Name,Where,OrderBy,Limit,Offset) -> 
  select(Name,Where,OrderBy,Limit,Offset).

init() ->
  create_tables([node()]).

models() ->
  lists:foldl(fun(E,Acc) ->
     Acc ++ [new_record(E)]
  end,[],records()).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

new_model(Name) ->
  new_model(fields(Name),new_record(Name)).

new_model([F|Fs],Record) ->
  R = set_value(F,undefined,Record),
  new_model(Fs,R);
new_model([],Record) ->
  Record.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

create_tables(Nodes) ->
  application:load(mnesia),

  %% Set mnesia dir
  MnesiaDir = case estore_utils:get_db_config(mnesia,dir) of
    [] ->
      filename:join(estore_utils:root_dir(),"mnesia_db");
    undefined ->
      filename:join(estore_utils:root_dir(),"mnesia_db");
    Dir ->
      Dir
  end,
  application:set_env(mnesia,dir,MnesiaDir),

  %% Set table persistance
  {Copies,LogMsg} = case estore_utils:get_db_config(mnesia,store) of
    ram_copies -> 
      {ram_copies,"in RAM"};
    disc_copies ->
      {disc_copies,"on DISC"};
    undefined ->
      {ram_copies,"in RAM"}
  end,
  
  %% Create schema
  case Copies of
    disc_copies ->
      case mnesia:create_schema(Nodes) of
	ok ->
	  estore_logging:log_term(info,"Schema has been created " ++ LogMsg);
	{error, {_,{already_exists,_Node}}} ->
	  estore_logging:log_term(info,"Schema already exists on node.");
	{error, Error} ->
	  estore_logging:log_term(error,{?MODULE,?LINE,Error})
      end;
    _ -> ok
  end,
  {ok,MnesiaPath} = application:get_env(mnesia,dir),
  estore_logging:log_term(info,"Mnesia DIR is: " ++ MnesiaPath),
  estore_logging:log_term(info,"Mnesia tables reside " ++ LogMsg),

  %% Start Mnesia
  application:start(mnesia),

  %% Create tables
  Tables = lists:foldl(fun(Table,Acc) -> 
    Acc ++ [{Table,[{attributes,fields(Table)},{Copies,Nodes}]}]
  end,[],records()),
  create_mnesia_table(Tables,[]).

%% @private {@link create_tables/1}. helper. 

create_mnesia_table([{Name,Atts}|Specs],Created) ->
  TableName = atom_to_list(Name),
  case mnesia:create_table(Name,Atts) of
    {aborted,{already_exists,Table}} ->
      Result = Created ++ [{ok,{already_exists,Table}}],
      estore_logging:log_term(info,TableName ++ " already exists.");
    {atomic,ok} -> 
      Result = Created ++ [{ok,{atomic,ok}}],
      estore_logging:log_term(info,"Table " ++ TableName ++ " created.");
    Error -> 
      Result = Created ++ [{error,Error}],
      estore_logging:log_term(info,{"Could not create " ++ TableName,Error})
  end,
  create_mnesia_table(Specs,Result);
create_mnesia_table([],Created) ->
  mnesia:wait_for_tables(records(),5000),
  {ok_error(Created),Created}.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

save_record(Record) when is_tuple(Record) ->
  Table = hd(tuple_to_list(Record)),
  Fun = fun() -> mnesia:write(Table,Record,write) end,
  case mnesia:transaction(Fun) of
    {atomic,ok} -> {ok,Record};
    {aborted,Reason} -> {error,{aborted,Reason}}
  end;
save_record(Records) when is_list(Records) ->
  Fun = fun() -> 
    [mnesia:write(hd(tuple_to_list(Rec)),Rec,write)|| Rec <- Records] 
  end,
  case mnesia:transaction(Fun) of
    {atomic,Results} -> {ok,Results};
    {aborted,Reason} -> {error,{aborted,Reason}}
  end.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

match(Name,Id) ->
  Fun = fun() -> mnesia:read(Name,Id) end,
  case mnesia:transaction(Fun) of
    {atomic,Result} -> Result;
    {aborted, Reason} -> {error, Reason}
  end.

%% @doc use match specification in where eg: [{'$1',[],['$1']}] to select all
%% records.
%%

select(Name,Where,OrderBy,Limit,Offset) ->
  MatchSpec = if Where =:= [all] -> [{'$1',[],['$1']}]; true -> Where end,
  RawList = mnesia:dirty_select(Name,MatchSpec),
  SortedList = if RawList /= [] ->
    Head = lists:nth(1,RawList),
    case is_record(Head,Name) of true ->
      order_by(RawList,OrderBy);
    false -> RawList end;
  true -> RawList end,
  SkippedList = offset(SortedList,Offset),
  limit(SkippedList,Limit).

order_by([],_OrdersBy) ->
  [];
order_by(List,[]) ->
  Key = lists:nth(1,fields(hd(tuple_to_list(hd(List))))),
  apply_order(List,[{Key,asc}]);
order_by(List,OrdersBy) ->
  apply_order(List,OrdersBy).

apply_order(Ordered,[{Field,AscDesc}|OrdersBy]) ->
  case AscDesc of
    asc ->
      Fun = fun (A,B) -> get_value(Field,A) =< get_value(Field,B) end,
      Sorted = lists:sort(Fun,Ordered);
    desc ->
      Fun = fun (A,B) -> get_value(Field,A) >= get_value(Field,B) end,
      Sorted = lists:sort(Fun,Ordered);
    _ ->
      Sorted = Ordered
    end,
  apply_order(Sorted,OrdersBy);
apply_order(Ordered,[]) ->
  Ordered.
  
offset(List,0) -> 
  List;
offset(List,Skip) when Skip >= length(List) -> 
  [];
offset(List,Skip) -> 
  lists:nthtail(Skip,List).

limit(List,all) ->
  List;
limit(List,Max) when is_integer(Max) ->
  lists:sublist(List,Max).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

convert_to_result(_Type,Val) ->
  Val.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

remove(Name,Id) ->
  mnesia:dirty_delete(Name,Id).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

ok_error([{ok,_}|Results]) ->
  ok_error(Results);
ok_error([]) ->
  ok;
ok_error([{error,_}|_Results]) ->
  error.




