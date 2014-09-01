-module(estore_pgsql).

-export([
  create_index/3
  ,create_index/4
  ,select/1
  ,select/2
  ,select/3
  ,select/4
  ,select/5
  ,select_index/1
  ,select_index/2
  ,transaction/1
  ,rollback/0
  ,drop_table/1
  ,drop_table/2
  ,drop_table/3
  ,table_info/3
  ,where/1
  ,where_to_string/1
]).

-export([
  init/0
  ,new/1
  ,models/0
  ,save/1
  ,delete/1
  ,find/2
]).

-export([
  squery/1
  ,squery/2
  ,equery/2
  ,equery/3
]).


-export([
  create_schema/1
  ,sql_create_schema/1

  ,schema_exists/1
  ,sql_schema_exists/1

  ,drop_schema/1
  ,sql_drop_schema/1

  ,create_tables/1
  ,sql_create_tables/1
  ,create_table/1
  ,sql_create_table/1
  ,sql_create_table/2
  ,sql_create_table/3

  ,drop_tables/0
  ,sql_one_to_many/3

  ,save_model/1
]).

-export([
  get_pool/0
]).

-include("estore.hrl").
-include("pgsql.hrl").

-compile({parse_transform,estore_dynarec}).

-define(SQUERY(Sql),squery(Sql)).
-define(EQUERY(Sql,Args),equery(Sql,Args)).
-define(SCHEMA,get_schema()).
-define(POOL,get_pool()).

%% -----------------------------------------------------------------------------
%% --------------------------------- API ---------------------------------------
%% -----------------------------------------------------------------------------

new(Name) ->
 new_model(Name).

save(Model) ->  
  save_model(Model).

delete(_Name) ->
  ok.

find(_Name,_Conditions) ->
  ok.

init() ->
  drop_schema(?SCHEMA),
  create_schema(?SCHEMA),
  create_tables(models()).

models() ->
  lists:foldl(fun(E,Acc) ->
     Acc ++ [new_record(E)]
  end,[],records()).

%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------

squery(Sql) ->
  squery(?POOL,Sql).
squery(PoolName,Sql) ->
  ?LOG(debug,Sql),
  poolboy:transaction(PoolName,fun(Worker) ->
    gen_server:call(Worker,{squery,Sql})
  end).

equery(Sql,Params) ->
  equery(?POOL,Sql,Params).
equery(PoolName,Sql,Params) ->
  ?LOG(debug,Sql),
  poolboy:transaction(PoolName,fun(Worker) ->
    gen_server:call(Worker,{equery,Sql,Params})
  end).


%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------

new_model(Name) ->
  new_model(fields(Name),new_record(Name)).

new_model([F|Fs],Record) ->
  R = case has_relation(Record,F) of
    {_RelType,Ref} -> new_model(F,Record,has_null(Record,F),Ref);
    undefined -> set_value(F,undefined,Record)
  end,
  new_model(Fs,R);
new_model([],Record) ->
  Record.

new_model(F,Record,{null,true},_Ref) ->
  set_value(F,null,Record);
new_model(F,Record,{null,false},Ref) ->
  set_value(F,new_model(Ref),Record).

%% -----------------------------------------------------------------------------
%% -------------------------------- SCHEMA -------------------------------------
%% -----------------------------------------------------------------------------

drop_schema(Schema) ->
  case schema_exists(Schema) of
    true -> ?SQUERY(sql_drop_schema(Schema));
    false -> ok
  end.

sql_drop_schema(Schema) ->
  sql_drop_schema(Schema,[{cascade,true}]).
sql_drop_schema(Schema,Opts) ->
  "DROP SCHEMA " ++ options_to_string({options,ifexists},Opts) ++ " "
  ++ value_to_string(Schema) ++ " "
  ++ options_to_string({options,cascade},Opts) ++ ";".

%% -----------------------------------------------------------------------------

create_schema(Schema) ->
  case schema_exists(Schema) of
    false -> ?SQUERY(sql_create_schema(Schema)); 
    true -> ok
  end.
    
sql_create_schema(Schema) ->
  sql_create_schema(Schema,[]).
sql_create_schema(Schema,Op) ->
  "CREATE SCHEMA " ++ options_to_string({options,ifexists},Op) 
  ++ " " ++ value_to_string(Schema) ++ ";".

%% -----------------------------------------------------------------------------

schema_exists(Schema) ->
  case ?SQUERY(sql_schema_exists(Schema)) of
    {ok,_ColInfo,[{SchemaBin}]} when is_binary(SchemaBin) -> true;
    {ok,_ColInfo,[]} -> false
  end.

sql_schema_exists(Schema) ->
  "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"
   ++  value_to_string(Schema)  ++ "';".

%% -----------------------------------------------------------------------------
%% -------------------------------- TABLES -------------------------------------
%% -----------------------------------------------------------------------------

create_tables(Records) ->
  Sql = sql_create_tables(Records),
  lists:foldl(fun(E,Acc) -> 
    Acc ++ [?SQUERY(E)]
  end,[],Sql).

sql_create_tables(Records) ->
  Tables = lists:foldl(fun(E,Acc) ->
    Acc ++ [sql_create_table(E)] ++ sql_create_relation_tables(E)
  end,[],Records),
  Tables.

sql_create_relation_tables(Record) ->
  Name = hd(tuple_to_list(Record)),
  lists:foldl(fun(E,Acc) -> 
    case sql_create_relation_table(Record,E) of
      [] -> Acc ++ [];
      Sql -> Acc ++ [Sql]
    end
  end,[],fields(Name)).

sql_create_relation_table(Record,Field) ->
  Table = hd(tuple_to_list(Record)),
  Constraints = estore_utils:get_value(constraints,get_value(Field,Record),[]),
  case has_relation(Record,Field) of
    {one_to_many,Ref} -> sql_one_to_many(Table,Ref,Constraints);
    {many_to_many,Ref} -> sql_many_to_many(Table,Ref,Constraints);
    _ -> []
  end.

create_table(Record) ->
  ?SQUERY(sql_create_table(Record)).

sql_create_table(Record) ->
  sql_create_table(Record,[{ifexists,false}]).

sql_create_table(Record,Options) ->
  Name = hd(tuple_to_list(Record)),
  Fields = strip_comma(convert_fields(Record)),
  sql_create_table(Name,Fields,Options).
sql_create_table(Name,Fields,Options) ->
  "CREATE TABLE " ++ options_to_string({options,ifexists},Options) ++ " " 
  ++ has_value(schema,?SCHEMA) ++ value_to_string(Name) 
  ++ " (" ++ Fields ++ "\n); ".

convert_fields(Record) ->
  Name = hd(tuple_to_list(Record)),
  lists:foldl(fun(E,Acc) ->
    FldOpts = get_value(E,Record),
    FldOpts2 = if E =:= id -> 
      FldOpts ++ [{constraints,[{pk,[]},{null,false}]}]; 
      true -> FldOpts end,
    FldTuple = {{Name,E},FldOpts2},
    FldStmt = case has_relation(Record,E) of
      undefined -> convert_field(FldTuple);
      {references,_Ref} -> convert_field(FldTuple); 
      _ -> []
    end,
    Acc ++ FldStmt
  end,[],fields(Name)).
  
convert_field({{Table,Field},FldOpts}) ->
  value_to_string(Field) ++ " " ++
  strip_comma(options_to_string({field,{Table,Field}},FldOpts)) ++ ", ".

%% -----------------------------------------------------------------------------

drop_tables() ->
  lists:foldl(fun(E,Acc) ->
    Acc ++ [drop_table(E)] 
  end,[],records()). 

drop_table(T) ->
  drop_table(?SCHEMA,T).
drop_table(S,T) ->
  drop_table(S,T,[{ifexists,true},{cascade,true}]).
drop_table(S,T,Op) ->
  "DROP TABLE " ++ options_to_string({options,ifexists},Op) ++ " " 
  ++ has_value(schema,S) ++ value_to_string(T) ++ " " 
  ++ options_to_string({options,cascade},Op) ++ ";".


%% -----------------------------------------------------------------------------
%% -------------------------- RELATIONS ----------------------------------------
%% -----------------------------------------------------------------------------

sql_many_to_many(_Table,_Ref,_Constraints) ->
  [].

sql_one_to_many(Table,Ref,_Constraints) ->
  LookupTableName = relation_table_name(Table,Ref) ,
  FieldTuples = [
    {{LookupTableName,Table},[
      {type,{'bigint',[]}}
      ,{constraints,[{references,Table},{null,false}]}
    ]}
    ,{{LookupTableName,Ref},[
      {type,{'bigint',[]}}
      ,{constraints,[{references,Ref},{null,false}]}
    ]}
  ],
  FieldsStr = lists:foldl(fun(E,Acc) -> 
    Acc ++ convert_field(E)
  end,[],FieldTuples),  
  Fields = strip_comma(FieldsStr),
  sql_create_table(LookupTableName,Fields,[{ifexists,false}]).


%% -----------------------------------------------------------------------------
%% ------------------------------- SAVE ----------------------------------------
%% -----------------------------------------------------------------------------


save_model(Model) ->
  %% get all records composing the model
  %% put the records in an order to run them observing constraint declarations
  %% check ofor the existence of id field for every record, if it is there create statement for update otherwise insert
  %% add any needed updates/inserts to the relation lookup tables and amend the order

%  has_relation(Record,FieldName)  
  OrderedRecords = model_records(Model),
  io:fwrite("~n~n ~p ~n~n",[OrderedRecords]),
  SqlPlan = sql_insert_list(OrderedRecords),
  Plan = sql_plan(SqlPlan),
  execute_sql_plan(Plan).
%  run_insert(Model).  


%% @doc Creates a list of records to be transformed into SQL statements.
%% The records in the list follow the order of transformation i.e. the list is 
%% ordered.

model_records(Model) ->
  List = lists:reverse(relation_record(Model)),
  List ++ [{Model,[]}].

relation_record(Model) ->
  Name = hd(tuple_to_list(Model)), 
  relation_records(Model,new_record(Name),fields(Name),[]).

relation_records(Record,RecordDef,[F|Fs],Result) ->
  Name = hd(tuple_to_list(Record)), 
  FieldVal = get_value(F,Record),
  case has_relation(RecordDef,F) of
    {references,_Ref} ->
      NewResult = Result ++ [{FieldVal,relation_record(FieldVal)}],
      relation_records(Record,RecordDef,Fs,NewResult);
    {one_to_many,Ref} ->
      NewResult = Result ++ [{relation_action,{Name,Ref}}] ++ [{FieldVal,relation_record(FieldVal)}],
      relation_records(Record,RecordDef,Fs,NewResult);
    _NoRelation -> 
      relation_records(Record,RecordDef,Fs,Result)
  end;
relation_records(_Record,_RecordDef,[],Result) ->
  Result.

%% @doc Turns records from the list into SQL statements preserving the order.

sql_insert_list(Records) ->
  sql_insert_list(Records,[]).

sql_insert_list([Record|Records],SqlPlan) ->
  Plan = case Record of 
    {relation_action,Opts} -> [{relation_action,Opts}];
    {Tuple,[]} -> [{sql_insert_values(Tuple),[]}];
    {Tuple,List} -> [{sql_insert_values(Tuple),sql_insert_list(List)}]
  end,
  sql_insert_list(Records,SqlPlan ++ Plan);
sql_insert_list([],SqlPlan) ->
  SqlPlan.

sql_insert_values(Record) ->
  Name = hd(tuple_to_list(Record)),
  {Insert,Value,Params} = sql_insert_values(Record,new_record(Name),fields(Name),[],[],[],1),
  {Name,{"INSERT INTO " ++ has_value(schema,?SCHEMA) ++ value_to_string(Name) ++ " ( " ++
  Insert ++ " ) VALUES ( " ++ Value ++ " ) RETURNING id;",Params}}. 

sql_insert_values(Record,RecordDef,[id|Fs],Insert,Value,Params,ParamCount) ->
  sql_insert_values(Record,RecordDef,Fs,Insert,Value,Params,ParamCount);
sql_insert_values(Record,RecordDef,[F|Fs],Insert,Value,Params,ParamCount) ->
  Type = estore_utils:get_value(type,get_value(F,RecordDef),undefined),
  case has_relation(RecordDef,F) of
    {references,Relation} -> 
      Inserts = Insert ++ value_to_string(F) ++ ", ",
      Count = ParamCount + 1,
      Formatted = "$" ++ integer_to_list(ParamCount)  ++ ", ",
      NewParams = Params ++ [{Relation,ParamCount}];
    {one_to_many,_Relation} -> 
      Inserts = Insert,
      Count = ParamCount,
      Formatted = "",
      NewParams = Params;
    _ ->
      Inserts = Insert ++ value_to_string(F) ++ ", ",
      Count = ParamCount,
      Formatted = format_to_sql(Type,get_value(F,Record)) ++ ", ",
      NewParams = Params
  end,
  Values = Value ++ Formatted,
  sql_insert_values(Record,RecordDef,Fs,Inserts,Values,NewParams,Count);
sql_insert_values(_Record,_RecordDef,[],Inserts,Values,Params,_ParamCount) ->
  {strip_comma(Inserts),strip_comma(Values),Params}.

format_to_sql(_Type,'undefined') ->
  value_to_string('null');
format_to_sql(_Type,'null') ->
  value_to_string('null');
format_to_sql('bigserial',Value) ->
  value_to_string(Value);
format_to_sql('integer',Value) ->
  value_to_string(Value);
format_to_sql(_Quoted,Value) ->
  "'" ++ value_to_string(Value) ++ "'".

sql_plan(SqlPlan) ->
  sql_plan(SqlPlan,[]).
sql_plan([{relation_action,Opts}|Sqls],Result) ->
  sql_plan(Sqls,Result ++ [{relation_action,Opts}]);
sql_plan([{{Name,Sql},[]}|Sqls],Result) ->
  sql_plan(Sqls,Result ++ [{Name,Sql}]);
sql_plan([{{Name,Sql},List}|Sqls],Result) ->
  sql_plan(Sqls,Result ++ sql_plan(List) ++ [{Name,Sql}]);
sql_plan([],Result) ->  
  Result.

execute_sql_plan(SqlPlan) ->
  execute_sql_plan(SqlPlan,[]).

%% Carry is empty for the relation_action so advance its position 
execute_sql_plan([{relation_action,Relations}|StmtTuples],[]) ->
  Stmts = StmtTuples ++ [{relation_action,Relations}],
  execute_sql_plan(Stmts,[]);

%% Carry is not empty so check if we have ids' for all Relations
%% if not advance relation_action's position 
execute_sql_plan([{relation_action,{Table,Ref}}|StmtTuples],Carry) ->
  TableId = estore_utils:get_value(Table,Carry,undefined), 
  RefId = estore_utils:get_value(Ref,Carry,undefined),
  if TableId /= undefined andalso RefId /= undefined ->
    NewCarry = Carry -- [{Table,Ref}], 
    NewStmtTuples = StmtTuples,
    Sql = "INSER INTO " ++ has_value(schema,?SCHEMA) 
    ++ relation_table_name(Table,Ref) ++ " VALUES ($1, $2);",
    ?EQUERY(Sql,[TableId,RefId]); 
  true ->
    NewStmtTuples = StmtTuples ++ [{relation_action,{Table,Ref}}],
    NewCarry = Carry
  end,
  execute_sql_plan(NewStmtTuples,NewCarry);

%% Carry is not empty, run statements
execute_sql_plan([{Head,{Sql,ParamTuples}}|StmtTuples],Carry) ->
  {NewCarry,Params} = get_parameters(Carry,{Head,ParamTuples},[]),
  {ok,_No,_Col,[{Id}]} = ?EQUERY(Sql,Params),
  execute_sql_plan(StmtTuples,NewCarry ++ [{Head,Id}]);

%% Done
execute_sql_plan([],Carry) ->
  Carry.   

get_parameters(Carry,{Head,[{ParamKey,_Pos}|ParamTuples]},ParamList) ->
  case estore_utils:get_value(ParamKey,Carry,undefined) of
    undefined -> 
      io:fwrite("WTF",[]), 
      NewCarry = Carry, 
      Params = [];
    Id ->
      NewCarry = Carry -- [{ParamKey,Id}],
      Params = ParamList ++ [Id]
  end,
  get_parameters(NewCarry,{Head,ParamTuples},Params);
get_parameters(Carry,{_Head,[]},ParamList) ->
  {Carry,ParamList}.

%% -----------------------------------------------------------------------------
%% --------------------------- CONVERTERS --------------------------------------
%% -----------------------------------------------------------------------------


options_to_string({field,{T,F}},FldOpts) ->
  Type = options_to_string(type,estore_utils:get_value(type,FldOpts,{'bigint',[]})),
  Type ++ " " ++ options_to_string({constraints,{T,F}},
    estore_utils:get_value(constraints,FldOpts,undefined));

options_to_string({options,cascade},true) ->
  "CASCADE";
options_to_string({options,ifexists},true) ->
  "IF " ++ value_to_string(exists); 
options_to_string({options,ifexists},false) ->
  "IF NOT " ++ value_to_string(exists);
options_to_string({options,Key},Options) when is_list(Options) ->
  options_to_string({options,Key},proplists:get_value(Key,Options));
options_to_string({options,_},undefined) ->
  [];

options_to_string({constraints,{_T,_F}},{references,Ref}) ->
  "REFERENCES " ++ value_to_string(Ref) ++ " (id) ";
options_to_string({constraints,{_T,_F}},{null,false}) ->
  value_to_string('not') ++ " " ++ value_to_string(null)++ " ";
options_to_string({constraints,{_T,_F}},{null,true}) ->
  value_to_string(null) ++ " ";
options_to_string({constraints,{_T,_F}},{pk,_Opts}) ->
  "PRIMARY KEY ";
options_to_string({constraints,{T,F}},undefined) ->
  options_to_string({constraints,{T,F}},{null,true});
options_to_string({constraints,{T,F}},ConstraintsList) ->
  lists:foldl(fun(E,Acc) -> 
    Acc ++ options_to_string({constraints,{T,F}},E)
  end,[],ConstraintsList);


options_to_string('varchar',{length,Size}) ->
   "(" ++  value_to_string(Size) ++ ")";
options_to_string('bigint',_Opts) -> 
  "";
options_to_string('integer',_Opts) -> 
  "";
options_to_string('date',_Opts) -> 
  "";
options_to_string('bigserial',_Opts) ->
  "";

options_to_string('type',{Type,FldOpts}) ->
  value_to_string(Type) ++ " " ++ lists:foldl(fun(E,Acc) -> 
    Acc ++ options_to_string(Type,E)
  end,[],FldOpts);

options_to_string(_Key,undefined) ->
  [].




%% -----------------------------------------------------------------------------
%% ----------------------------- UTILITIES -------------------------------------
%% -----------------------------------------------------------------------------

get_schema() ->
  case estore_utils:get_db_config(pgsql,tablespace) of
    undefined -> estore;
    Schema -> Schema
  end.

get_pool() ->
  Pools = estore_utils:get_db_config(pgsql,pools),
  {Name,_} = lists:nth(1,Pools),
  Name.


has_relation(Record,FieldName) ->
  Constraints = estore_utils:get_value(constraints,get_value(FieldName,Record),[]),
  MaybeOneToMany = estore_utils:get_value(one_to_many,Constraints,undefined),
  maybe_one_to_many(Constraints,MaybeOneToMany). 
maybe_one_to_many(Constraints,undefined) ->
  MaybeManyToMany = estore_utils:get_value(many_to_many,Constraints,undefined),
  maybe_many_to_many(Constraints,MaybeManyToMany);
maybe_one_to_many(_Constraints,Ref) ->
  {one_to_many,Ref}.
maybe_many_to_many(Constraints,undefined) ->
  MaybeOneToOne = estore_utils:get_value(references,Constraints,undefined),
  maybe_references(Constraints,MaybeOneToOne);
maybe_many_to_many(_Constraints,Ref) ->
  {many_to_many,Ref}.
maybe_references(_Constraints,undefined) ->
  undefined;
maybe_references(_Constraints,Ref) ->
  {references,Ref}.

relation_table_name(Table,Ref) ->
  value_to_string(Table) ++ "_" ++ value_to_string(Ref).

has_null(Constraints) ->
  case estore_utils:get_value(null,Constraints,false) of
    true -> {null,true};
    false -> {null,false}
  end.

has_null(Record,FieldName) -> 
  Constraints = estore_utils:get_value(constraints,get_value(FieldName,Record),[]),
  has_null(Constraints).

strip_comma(String) ->
  string:strip(string:strip(String,both,$ ),both,$,).



%% -----------------------------------------------------------------------------


















select({S,T,A}) ->
  select([{S,T,A}]);
select({S,T}) ->
  select({S,T,undefined});
select(T) when is_atom(T) ->
  select({undefined,T,undefined});
select(Ts) when is_list(Ts) ->
  select(Ts,['*']).

select({S,T,A},Fs) when is_atom(T) ->
  select({S,T,A},Fs,undefined,undefined);
select({S,T},Fs) when is_atom(T) ->
  select({S,T,undefined},Fs,undefined,undefined);
select(T,Fs) when is_atom(T) ->
  select({undefined,T,undefined},Fs,undefined,undefined);
select(Ts,Fs) when is_list(Ts) ->
  select(Ts,Fs,undefined,undefined).

select({S,T,A},Fs,W) when is_atom(T) ->
  select({S,T,A},Fs,W,undefined);
select({S,T},Fs,W) when is_atom(T) ->
  select({S,T,undefined},Fs,W,undefined);
select(T,Fs,W) when is_atom(T) ->
  select({undefined,T,undefined},Fs,W,undefined);
select(Ts,Fs,W) when is_list(Ts) ->
  select(Ts,Fs,W,undefined).
  
select({S,T,A},Fs,W,G) when is_atom(T) -> 
  select([{S,T,A}],Fs,undefined,W,G);
select({S,T},Fs,W,G) when is_atom(T) -> 
  select({S,T,undefined},Fs,undefined,W,G);
select(T,Fs,W,G) when is_atom(T) -> 
  select({undefined,T,undefined},Fs,undefined,W,G);
select(Ts,Fs,W,G) when is_list(Ts) -> 
  select(Ts,Fs,undefined,W,G).

select({S,T,A},Fs,J,W,G) ->
  select([{S,T,A}],Fs,J,W,G);
select({S,T},Fs,J,W,G) when is_atom(T) ->
  select({S,T,undefined},Fs,J,W,G);
select(T,Fs,J,W,G) when is_atom(T) ->
  select({undefined,T,T},Fs,J,W,G);
select(Ts,Fs,J,W,G) when is_list(Ts) ->
  "SELECT " ++ field_to_string(Fs) ++ " FROM " 
  ++ " " ++ join(J) ++ "" ++ where(W) ++ groupby(G) ++ ";".
 
create_index(N,T,Cs) when is_atom(T) ->
  create_index(N,T,Cs,undefined).
create_index(N,T,Cs,Ops) when is_atom(T) ->
  create_index(N,{undefined,T},Cs,Ops);
create_index(N,{S,T},Cs,Ops) ->
  "CREATE INDEX " ++ options_to_string(nolock,Ops) ++ " " ++ value_to_string(N) 
  ++ " ON " ++ has_value(schema,S) ++ value_to_string(T) 
  ++ " (" ++ field_to_string(Cs) ++ ");".

select_index(I) ->
  select_index(undefined,I).
select_index(S,I) ->
  Tables = [{undefined,pg_class,t},{undefined,pg_class,i},{undefined,pg_index,ix}
    ,{undefined,pg_attribute,a},{undefined,pg_namespace,n}],
  Fields = [{n,nspname},{i,relname}],
  Where = [{{t,oid},'=',{ix,indexrelid}}
    ,'AND',{{t,relnamespace},'=',{n,oid}}
    ,'AND',{{i,oid},'=',{ix,indexrelid}}
    ,'AND',{{n,nspname},'LIKE',value_to_string(S) ++ "%"}
    ,'AND',{{t,relname},'LIKE',value_to_string(I) ++ "%"}],
  select(Tables,Fields,Where,Fields).

transaction(Stmt) ->
  "BEGIN;\n " ++ Stmt ++ "COMMIT;\n".

rollback() ->
  "ROLLBACK;".

table_info(D,S,T) ->
  select({information_schema,columns},[column_name,ordinal_position,data_type
    ,is_nullable,character_maximum_length,numeric_precision,numeric_scale]
    ,[{table_catalog,'=',value_to_string(D)},'AND',{table_schema,'=',value_to_string(S)}
    ,'AND',{table_name,'=',value_to_string(T)}]).

%% ------------------------------------------------------------------------------


%% GROUP BY

groupby(undefined) ->
  [];
groupby(G) when is_list(G) ->
  " GROUP BY " ++ field_to_string(G).

%% JOIN
%% [{{'LEFT OUTER JOIN',{schema,othertable,o}},'ON',{{'T',name},'=',{'O',name}}}]

join(undefined) ->
  [];
join(J) when is_list(J) ->
  lists:foldl(fun(E,Acc) -> Acc ++ " " ++ join_to_string(E) end,[],J).

join_to_string(V) when is_atom(V) ->
  value_to_string(V);
join_to_string({J,{T,A}}) when is_atom(J) ->
  join_to_string({J,{undefined,T,A}});
join_to_string({J,{S,T,_A}}) when is_atom(J) ->
  value_to_string(J) ++ has_value(schema,S) ++ value_to_string(T) ++ " ";
join_to_string({J,C}) when is_atom(J) andalso is_atom(C) ->
  where_to_string({J,C});
join_to_string({F1,T,F2}) ->
  where_to_string({F1,T,F2}).

%% WHERE
%% [{{'C',gender},'=',"M"},'AND',[{{'C',age},'=',20},'OR',{{'C',age},'=',25}],'AND',{{'C',name},'LIKE',{'S',name}}]

where(undefined) ->
  [];
where(W) when is_list(W) -> 
  " WHERE " ++ lists:foldl(fun(E,Acc) -> Acc ++ " " ++ where_to_string(E) end,[],W).

where_to_string(V) when is_atom(V) ->
  value_to_string(V);
where_to_string({J,C}) when is_atom(J) ->
  value_to_string(J) ++ "." ++ value_to_string(C);
where_to_string(V) when is_integer(V) ->
  value_to_string(V);
where_to_string(V) when is_float(V) ->
  value_to_string(V); 
where_to_string({F1,T,F2}) ->
  where_to_string(F1) ++ " " ++ where_to_string(T) ++ " " ++ where_to_string(F2);  
where_to_string(V) when is_list(V) ->
  case io_lib:printable_unicode_list(V) of
    false ->
      "(" ++ lists:foldl(fun(E,Acc) -> Acc ++ " " ++ where_to_string(E) end,[],V) ++ ")";
    true ->
       "'" ++ V ++ "'"
  end.

field_to_string(#{name := N,type := T,length := L,null := I} = _F) ->
  NStr = value_to_string(N), TStr = value_to_string(T),
  if NStr /= [] andalso TStr /= [] -> 
    NStr ++ " " ++ TStr ++ " " ++ has_value(length,L) ++ " " ++ has_value(null,I);
  true -> [] end;
field_to_string(Fs) when is_list(Fs) ->
  field_to_string(Fs,hd(Fs));
field_to_string(Fs) when is_atom(Fs) ->
  value_to_string(Fs).

field_to_string(Fs,F1) when is_atom(F1) ->
  string:strip(lists:foldl(fun(E,Acc) -> Acc ++ "," ++ atom_to_list(E) end,[],Fs),left,$,);
field_to_string(Fs,F1) when is_tuple(F1) ->
  string:strip(lists:foldl(fun(E,Acc) -> Acc ++ "," ++ where_to_string(E) end,[],Fs),left,$,);
field_to_string(Fs,F1) when is_map(F1) ->
  string:strip(lists:foldl(fun(E,Acc) -> Acc ++ ",\n" ++ field_to_string(E) end,[],Fs),left,$,).

value_to_string(V) when is_atom(V) andalso V /= undefined ->
  atom_to_list(V); 
value_to_string(V) when is_integer(V) ->
  integer_to_list(V);
value_to_string(V) when is_float(V) ->
  float_to_list(V,[{decimals,12},compact]);
value_to_string({Mega,S,Micro}) when is_integer(S) ->
  integer_to_list(Mega) ++ integer_to_list(S) ++ integer_to_list(Micro);
value_to_string(V) when is_list(V) ->
  V;
value_to_string(_V) ->
  [].

has_value(length,{L,P}) ->
  "(" ++ value_to_string(L) ++ "," ++ value_to_string(P) ++ ")";
has_value(length,V) when V /= undefined ->
  "(" ++ value_to_string(V) ++ ")";
has_value(schema,V) when V /= undefined ->
  value_to_string(V) ++ ".";
has_value(odc,V) when V /= undefined ->
  "ON DELETE " ++ proplists:get_value(cascade,options_map());
has_value(null,V) when V =:= no orelse V =:= undefined ->
  "NOT NULL";
has_value(_,_) ->
  [].

%default_name({Key,T},N) when N =:= undefined->
%  value_to_string(Key) ++ "_" ++ value_to_string(T) ++ "_" 
%    ++ value_to_string(now());
%default_name({_Key,_T},N) ->
%  value_to_string(N).

%add_constraint(S,N,Cs) when Cs /= undefined ->
%  lists:foldl(fun(E,Acc) -> Acc ++ 
%    "ALTER TABLE " ++ has_value(schema,S) ++ value_to_string(N) 
%    ++ " ADD CONSTRAINT " ++ constraint_to_string(N,E)
%  ++ "\n" end,[],Cs);
%add_constraint(_S,_N,_C) ->
%  [].
 
%constraint_to_string(T,#pk{id=N,fields=Fs} = _C) ->
%  default_name({pk,T},N) ++ " PRIMARY KEY (" ++ field_to_string(Fs) ++ ");";
%constraint_to_string(T,#unique{id=N,fields=Fs} = _C) ->
%  default_name({uq,T},N) ++ " UNIQUE (" ++ field_to_string(Fs) ++ ");";
%constraint_to_string(T,#fk{id=N,on_delete_cascade=Odc,fields=Fs,r_schema=Rs,r_table=Rt,r_fields=RFs} = _C) ->
%  default_name({fk,T},N) ++ " FOREIGN KEY (" ++ field_to_string(Fs) 
%  ++ ") REFERENCES " ++  has_value(schema,Rs) ++ value_to_string(Rt) ++ " (" ++ field_to_string(RFs) 
%  ++ ") " ++ has_value(odc,Odc) ++ ";".
%
%options_to_string(K,Opts) when is_list(Opts) ->
%  case lists:member(K,Opts) of true -> proplists:get_value(K,options_map()); _ -> [] end;
%options_to_string(_K,_Opts) ->
%  [].

options_map() -> [
    {nolock,"CONCURRENTLY"}
    ,{cascade,"CASCADE"}
    ,{ifexists,"IF EXISTS"}
    ,{ifnotexists,"IF NOT EXISTS"}
  ].


  

