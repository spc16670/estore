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

  ,select/2
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
  save_record(Model).

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
    {_RelType,_Ref} ->  set_value(F,undefined,Record);%set_value(F,new_model(Ref),Record);
    undefined -> set_value(F,undefined,Record)
  end,
  new_model(Fs,R);
new_model([],Record) ->
  Record.

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
  ++ table_name(Name) ++ " (" ++ Fields ++ "\n); ".

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

drop_table(Table) ->
  drop_table(Table,[{ifexists,true},{cascade,true}]).
drop_table(Table,Op) ->
  "DROP TABLE " ++ options_to_string({options,ifexists},Op) ++ " " 
  ++ table_name(Table) ++ " "  ++ options_to_string({options,cascade},Op) ++ ";".

%% -----------------------------------------------------------------------------
%% -------------------------- RELATIONS ----------------------------------------
%% -----------------------------------------------------------------------------

sql_many_to_many(_Table,_Ref,_Constraints) ->
  [].

%% -----------------------------------------------------------------------------
%% ------------------------------- SAVE ----------------------------------------
%% -----------------------------------------------------------------------------

save_record(Record) when is_tuple(Record) ->
  case get_value(id,Record) of
    undefined -> save_insert(Record);
    Id when is_integer(Id) -> save_update(Record)
  end;
save_record(Records) when is_list(Records) ->
  lists:foldl(fun(Record,Acc) -> Acc ++ [save_record(Record)] end,[],Records).

%% -----------------------------------------------------------------------------

save_insert(Record) ->
  case ?SQUERY(sql_insert(Record)) of
    {ok,_Count,_Cols,[{Id}]} -> {ok,binary_to_integer(Id)};
    Error -> {error,Error}
  end.

sql_insert(Record) ->
  Name = hd(tuple_to_list(Record)),
  sql_insert(Record,new_record(Name),fields(Name),[],[]).

sql_insert(Record,RecordDef,[id|Fs],Insert,Value) ->
  sql_insert(Record,RecordDef,Fs,Insert,Value);
sql_insert(Record,RecordDef,[F|Fs],Insert,Value) ->
  Type = estore_utils:get_value(type,get_value(F,RecordDef),undefined),
  Inserts = Insert ++ value_to_string(F) ++ ", ",
  Formatted = format_to_sql(Type,get_value(F,Record)) ++ ", ",
  Values = Value ++ Formatted,
  sql_insert(Record,RecordDef,Fs,Inserts,Values);
sql_insert(Record,_RecordDef,[],Inserts,Values) -> 
  Name = hd(tuple_to_list(Record)),
  "INSERT INTO " ++ table_name(Name) ++ " ( " ++ strip_comma(Inserts) ++
   " ) VALUES ( " ++ strip_comma(Values) ++ " ) RETURNING id;". 

%% -----------------------------------------------------------------------------

save_update(Record) ->
  case ?SQUERY(sql_update(Record)) of
    {ok,Count} -> {ok,Count};
    Error -> {error,Error}
  end. 

sql_update(Record) ->
  Name = hd(tuple_to_list(Record)),
  sql_update(Record,new_record(Name),fields(Name),[]).

sql_update(Record,RecordDef,[id|Fs],Update) ->
  sql_update(Record,RecordDef,Fs,Update);
sql_update(Record,RecordDef,[F|Fs],Update) ->
  Type = estore_utils:get_value(type,get_value(F,RecordDef),undefined),
  Formatted = format_to_sql(Type,get_value(F,Record)) ++ ", ",
  Updates = value_to_string(F) ++ " = " ++ Formatted,
  sql_update(Record,RecordDef,Fs,Update ++ Updates);
sql_update(Record,_RecordDef,[],Updates) -> 
  Name = hd(tuple_to_list(Record)),
  Id = format_to_sql(integer,get_value(id,Record)),
  "UPDATE " ++ table_name(Name) ++ " SET " ++ strip_comma(Updates) ++
   " WHERE id = " ++ Id  ++ "". 

%% -----------------------------------------------------------------------------
%% ----------------------------- SELECT ----------------------------------------
%% -----------------------------------------------------------------------------

select(Name,Where) ->
  Sql = select_sql(Name,Where),
  case ?SQUERY(Sql) of
    {ok,Cols,Vals} ->
      select_to_record(Name,Cols,Vals);
    Error -> {error,Error}
  end.

select_sql(Name,Where) ->
  "SELECT * FROM " ++ table_name(Name) ++ " WHERE " ++ where(Where).

where(WhereList) ->
  Where = lists:foldl(fun({Field,Op,Cond},Acc) -> 
    Acc ++ value_to_string(Field) ++ " " ++  
    value_to_string(Op) ++ " " ++  
    value_to_string(Cond) ++ ", " 
  end,[],WhereList),
  strip_comma(Where).

select_to_record(Name,Cols,Vals) when is_list(Vals) ->
  lists:foldl(fun(Tuple,Acc) -> Acc ++ select_to_record(Name,Cols,Tuple) end,[],Vals);  

select_to_record(Name,Cols,Tuple) when is_tuple(Tuple) ->
  Fields = [X || {_,X,_,_,_,_} <- Cols],
  Vals = tuple_to_list(Tuple),
  PropList = lists:zip(Fields,Vals),
  results_to_record(new_model(Name),new_record(Name),fields(Name),PropList).

results_to_record(Record,RecordDef,[Field|Fields],PropList) ->
  FieldBin = atom_to_binary(Field,utf8),
  BinVal = estore_utils:get_value(FieldBin,PropList,undefined),
  Type = estore_utils:get_value(type,get_value(Field,RecordDef),undefined),
  Val = convert_to_result(Type,BinVal),
  NewRecord = set_value(Field,Val,Record),
  results_to_record(NewRecord,RecordDef,Fields,PropList);
results_to_record(Record,_RecordDef,[],_PropList) ->
  Record.

%% -----------------------------------------------------------------------------
%% --------------------------- CONVERTERS --------------------------------------
%% -----------------------------------------------------------------------------

convert_to_result(_Type,null) ->
  null;
convert_to_result({integer,_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({bigserial,_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({bigint,_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({serial,_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({varchar,_Opts},Val) ->
  binary_to_list(Val);
convert_to_result({null,_Opts},_Val) ->
  undefined;
convert_to_result(_Type,Val) ->
  Val.

format_to_sql(_Type,'undefined') ->
  value_to_string('null');
format_to_sql(_Type,'null') ->
  value_to_string('null');
format_to_sql({'bigserial',_Opts},Value) ->
  value_to_string(Value);
format_to_sql({'bigint',_Opts},Value) ->
  value_to_string(Value);
format_to_sql({'integer',_Opts},Value) ->
  value_to_string(Value);
format_to_sql(_Quoted,Value) ->
  "'" ++ value_to_string(Value) ++ "'".


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
  "REFERENCES " ++ table_name(Ref) ++ " (id) ";
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

%relation_table_name(Table,Ref) ->
%  value_to_string(Table) ++ "_" ++ value_to_string(Ref).

table_name(Name) ->
  has_value(schema,?SCHEMA) ++ value_to_string(Name). 

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


