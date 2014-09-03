-module(estore_pgsql).

-export([
  create_index/3
  ,create_index/4
  ,select_index/1
  ,select_index/2
  ,transaction/1
  ,rollback/0
  ,drop_table/1
  ,drop_table/2
  ,drop_table/3
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

  ,select/2
  ,run_select/2
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

find(Name,Conditions) ->
  select(Name,Conditions).

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
  ?LOG(debug,[Sql,Params]),
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
    {_RelType,Ref} -> set_value(F,new_model(Ref),Record); %new_model(F,Record,has_null(Record,F),Ref);
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
  OrderedRecords = model_records(Model),
  SqlPlan = sql_save_list(OrderedRecords),
  Plan = sql_plan(SqlPlan),
  execute_sql_plan(Plan).

%% @doc Creates a list of records to be transformed into SQL statements.
%% The records in the list follow the order of sql execution i.e. the list is 
%% ordered.

model_records(Model) ->
  List = lists:reverse(relation_record(Model)),
  List ++ [{Model,[]}].

relation_record(Model) when is_tuple(Model) ->
  Name = hd(tuple_to_list(Model)), 
  relation_records(Model,new_record(Name),fields(Name),[]).


relation_record(Model,Rel) when is_tuple(Model) ->
  [{relation_action,Rel}] ++ [{Model,relation_record(Model)}];
relation_record(Models,Rel) when is_list(Models) ->
  lists:foldl(fun(Record,Acc) -> 
    Acc ++ [{relation_action,Rel}] ++ [{Record,relation_record(Record)}]
  end,[],Models).
  

relation_records(Record,RecordDef,[F|Fs],Result) ->
  Name = hd(tuple_to_list(Record)), 
  FieldVal = get_value(F,Record),
  case has_relation(RecordDef,F) of
    {references,_Ref} ->
      NewResult = Result ++ [{FieldVal,relation_record(FieldVal)}],
      relation_records(Record,RecordDef,Fs,NewResult);
    {one_to_many,Ref} ->
      NewResult = Result ++ relation_record(FieldVal,{Name,Ref}),
      relation_records(Record,RecordDef,Fs,NewResult);
    _NoRelation -> 
      relation_records(Record,RecordDef,Fs,Result)
  end;
relation_records(_Record,_RecordDef,[],Result) ->
  Result.

%% @doc Turns records from the list into SQL statements preserving the order.

sql_save_list(Records) ->
  sql_save_list(Records,[]).

sql_save_list([Record|Records],SqlPlan) ->
  Plan = case Record of 
    {relation_action,Opts} -> [{relation_action,Opts}];
    {Tuple,[]} -> [{sql_save_values(Tuple),[]}];
    {Tuple,List} -> [{sql_save_values(Tuple),sql_save_list(List)}]
  end,
  sql_save_list(Records,SqlPlan ++ Plan);
sql_save_list([],SqlPlan) ->
  SqlPlan.

%% @doc Turns a record into a {RecordAtom,{SqlString,ParamList}} tuple.

sql_save_values(Record) when is_tuple(Record) ->
  case get_value(id,Record) of
    undefined -> 
      sql_insert_values(Record);
    _Id ->
      sql_update_values(Record)
  end.

%% @private INSERT ignores record ids.

sql_insert_values(Record) ->
  Name = hd(tuple_to_list(Record)),
  sql_insert_values(Record,new_record(Name),fields(Name),[],[],[],1).

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
sql_insert_values(Record,_RecordDef,[],Inserts,Values,Params,_ParamCount) -> 
  Name = hd(tuple_to_list(Record)),
  {Name,{"INSERT INTO " ++ table_name(Name) ++ " ( " ++ strip_comma(Inserts) ++
   " ) VALUES ( " ++ strip_comma(Values) ++ " ) RETURNING id;",Params}}. 

%% @doc UPDATE statements..

sql_update_values(_Record) ->
  ok.

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

%% @private Carry is empty for the relation_action so advance its position 
%% There are some StmtTuples needing running first.

execute_sql_plan([{relation_action,Relations}|StmtTuples],[]) ->
  Stmts = StmtTuples ++ [{relation_action,Relations}],
  execute_sql_plan(Stmts,[]);

%% @private Carry is not empty for relation action so check if we have ids' for 
%% all Relations if not advance relation_action's position.

execute_sql_plan([{relation_action,{Table,Ref}}|StmtTuples],Carry) ->
  TableId = estore_utils:get_value(Table,Carry,undefined), 
  RefId = estore_utils:get_value(Ref,Carry,undefined),
  if TableId /= undefined andalso RefId /= undefined ->
    NewCarry = Carry -- [{Ref,RefId}], 
    NewStmtTuples = StmtTuples,
    Sql = "INSERT INTO " ++ has_value(schema,?SCHEMA) 
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
%% ----------------------------- SELECT ----------------------------------------
%% -----------------------------------------------------------------------------

select(ModelName,Where) ->
  run_select(ModelName,Where).

run_select(Name,Where) ->
  "SELECT * FROM " ++ table_name(Name) ++ " WHERE " ++ where(Where).

%run_select(ModelName,Where) ->
%  {From,Record} = from(ModelName),
%  Sql = "SELECT * FROM " ++ From ++ " WHERE " ++ where(Where), 
%  Query = case ?SQUERY(Sql) of
%    {ok,ColList,[ValTuple]} ->
%      Vals = tuple_to_list(ValTuple),
%      Cols = [X || {_,X,_,_,_,_} <- ColList],
%      {ok,lists:zip(Cols,Vals)};
%    Error -> {error,Error}
%  end,
%  set_results(Query,Record).
%
%
%set_results({ok,Results},Record) ->
%  Name = hd(tuple_to_list(Record)), 
%  io:fwrite("~nResults ~p RECIRD ~p~n~n",[Results,Record]),
%  {_Res,Record} = set_results(fields(Name),Results,Record,new_record(Name)),
%  Record;
%set_results({error,Error},_Record) ->
%  Error.
%
%set_results([F|Fs],[{_Col,Val}|Results],Record,RecordDef) ->
%  io:fwrite("Setting ~p with ~p~n",[F,Val]),
%  case has_relation(RecordDef,F) of
%    {one_to_many,_Relation} ->
%      FVal = get_value(F,Record),
%      Name = hd(tuple_to_list(FVal)),
%      case is_tuple(FVal) of
%	true ->
%	  {Rec,Res} = set_results(fields(Name),Results,FVal,new_record(Name)),
%	  NewRec = set_value(F,Rec,Record), 
%	  set_results(Fs,Res,NewRec,RecordDef);
%	false ->
%          NewRec = set_value(F,undefined,Record), 
%          set_results(Fs,Results,NewRec,RecordDef)
%      end;
%    _ ->
%      NewRec = set_value(F,Val,Record), 
%      set_results(Fs,Results,NewRec,RecordDef)
%  end;
%set_results([],Res,Record,_RecordDef) ->
%  {Record,Res}.
%
%
%from(TableName) ->
%  from(new_record(TableName),fields(TableName),[]).
%
%from(RecordDef,[F|Fs],Sql) ->
%  case has_relation(RecordDef,F) of
%    {references,Relation} -> 
%      NewRec = set_value(F,new_model(Relation),RecordDef),
%      NewFrom = value_to_string(Relation) ++ ", ";
%    {one_to_many,Relation} ->  
%      NewRec = set_value(F,new_model(Relation),RecordDef),
%      NewFrom = value_to_string(Relation) ++ ", ";
%    _ ->
%      NewRec = set_value(F,undefined,RecordDef),
%      NewFrom = ""
%  end,
%  from(NewRec,Fs,Sql ++ NewFrom);
%from(RecordDef,[],Sql) ->
%  Name = hd(tuple_to_list(RecordDef)),
%  From = value_to_string(Name) ++ ", " ++ 
%  strip_comma(Sql),
%  {From,RecordDef}.
%
where(WhereList) ->
  Where = lists:foldl(fun({Field,Op,Cond},Acc) -> 
    Acc ++ value_to_string(Field) ++  
    value_to_string(Op) ++  
    value_to_string(Cond) ++ ", " 
  end,[],WhereList),
  strip_comma(Where).

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

value_to_string(V) when is_atom(V) andalso V /= undefined ->
  atom_to_list(V); 
value_to_string(V) when is_integer(V) ->
  integer_to_list(V);
value_to_string({Mega,S,Micro}) when is_integer(S) ->
  integer_to_list(Mega) ++ integer_to_list(S) ++ integer_to_list(Micro);
value_to_string(V) when is_list(V) ->
  V;
value_to_string(_V) ->
  [].

has_value(schema,V) when V /= undefined ->
  value_to_string(V) ++ ".".

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

table_name(Name) ->
  has_value(schema,?SCHEMA) ++ value_to_string(Name). 

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


 
create_index(N,T,Cs) when is_atom(T) ->
  create_index(N,T,Cs,undefined).
create_index(N,T,Cs,Ops) when is_atom(T) ->
  create_index(N,{undefined,T},Cs,Ops);
create_index(N,{S,T},_Cs,Ops) ->
  "CREATE INDEX " ++ options_to_string(nolock,Ops) ++ " " ++ value_to_string(N) 
  ++ " ON " ++ has_value(schema,S) ++ value_to_string(T) 
  ++ " (" ++ "Fields here - cs" ++ ");".

select_index(I) ->
  select_index(undefined,I).
select_index(_S,_I) ->
  "SELECT n.nspname, i.relname FROM pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n 
  WHERE t.oid = ix.relname 
  AND t.relnamespace = n.oid
  AND i.oid = ix.indexrelid
  AND n.nspname LIKE lamazone%
  AND t.relanme LIKE lamazone%".

transaction(Stmt) ->
  "BEGIN;\n " ++ Stmt ++ "COMMIT;\n".

rollback() ->
  "ROLLBACK;".


%% ------------------------------------------------------------------------------


