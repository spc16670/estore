-module(estore_mnesia).

-export([
  init/0
  ,new/1
  ,models/0
  ,save/1
  ,delete/2
  ,find/2
  ,find/5
]).

-export([
  create_tables/1
  ,save_record/1
  ,select/2
  ,select/5
]).

-include_lib("stdlib/include/qlc.hrl").
-include("mnesia.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

new(Name) ->
 new_model(Name).

save(Model) ->
  save_record(Model).

delete(_Name,_Conditions) ->
  ok.

find(Name,Id) when is_integer(Id) ->
  select(Name,Id);
find(Name,Conditions) when is_list(Conditions) ->
  select(Name,Conditions).

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
      filename:join(estore_utils:root_dir(), "db");
    undefined ->
      filename:join(estore_utils:root_dir(), "db");
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
  application:start(mnesia),

  %% Create tables
  Tables = lists:foldl(fun(Table,Acc) -> 
    Acc ++ [{Table,[{attributes,fields(Table)},{Copies,Nodes}]}]
  end,[],records()),
  create_mnesia_table(Tables,[]),
  mnesia:wait_for_tables(records(), 5000).

%% @private {@link create_tables/1}. helper. 

create_mnesia_table([{Name,Atts}|Specs],_Created) ->
  TableName = atom_to_list(Name),
  case mnesia:create_table(Name,Atts) of
    {aborted,Reason} ->
      estore_logging:log_term(info,{"Could not create " ++ TableName,Reason});
    {atomic,ok} -> 
      estore_logging:log_term(info,"Table " ++ TableName ++ " created.")
  end,
  create_mnesia_table(Specs,_Created);
create_mnesia_table([],_Created) ->
  ok.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

save_record(Record) ->
  Table = hd(tuple_to_list(Record)),
  Fun = fun() -> mnesia:write(Table,Record,write) end,
  case mnesia:transaction(Fun) of
    {atomic, ok} -> {ok,Record};
    {aborted, Reason} -> {error, Reason}
  end.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

select(_Name,_Id) ->
  ok.

select(_Name,_Where,_OrderBy,_Limit,_Offset) ->
  ok.



