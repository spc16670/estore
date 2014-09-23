-module(estore_pgsql).

-export([
  create_index/2
  ,drop_index/1
  ,drop_index/2
  ,drop_table/1
  ,drop_table/2
  ,transaction/1
  ,rollback/0
]).

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
  ,select/5
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

delete(Name,Conditions) ->
  ?SQUERY(delete_sql(Name,Conditions)).

find(Name,Id) when is_integer(Id) -> 
  select(Name,Id);
find(Name,Conditions) when is_list(Conditions) ->
  select(Name,Conditions).

find(Name,Where,OrderBy,Limit,Offset) ->
  select(Name,Where,OrderBy,Limit,Offset).

init() ->
  drop_schema(?SCHEMA),
  create_schema(?SCHEMA),
  create_tables(models()),
  create_index(shopper,[lname,dob]),
  create_index(address,postcode),
  create_index(user,[email,password]).

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
  R = set_value(F,undefined,Record),
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
    Acc ++ [sql_create_table(E)]
  end,[],Records),
  Tables.

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
    FldTuple = {{Name,E},FldOpts},
    FldStmt = convert_field(FldTuple),
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
  ++ table_name(Table) ++ " " ++ options_to_string({options,cascade},Op) ++ ";".

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

select(Name,Id) when is_integer(Id) ->
  select(Name,[{'id','=',Id}],[],50,0);
select(Name,Where) when is_list(Where) ->
  select(Name,Where,[],50,0).

select(Name,Where,OrderBy,Limit,Offset) ->
  Sql = select_sql(Name,Where,OrderBy,Limit,Offset),
  case ?SQUERY(Sql) of
    {ok,Cols,Vals} ->
      select_to_record(Name,Cols,Vals);
    Error -> {error,Error}
  end.

select_sql(Name,Where,OrderBy,Limit,Offset) ->
  "SELECT * FROM " ++ table_name(Name) ++ where(Name,Where) ++ order_by(OrderBy)
  ++ case Limit of all -> ""; _ -> " LIMIT " ++ value_to_string(Limit) end 
  ++ case Offset of 0 -> ""; _ -> " OFFSET " ++ value_to_string(Offset) end.

order_by([]) ->
  [];
order_by(OrdersBy) ->
  OrderLst = lists:foldl(fun({Field,Sort},Acc) ->
    Acc ++ value_to_string(Field) ++ " " ++
    value_to_string(Sort) ++ ", "
  end,[],OrdersBy),
  " ORDER BY " ++ strip_comma(OrderLst).

where(_Name,[]) ->
  [];
where(Name,WhereList) ->
  Where = lists:foldl(fun(Cond,Acc) -> 
    Acc ++ case Cond of
      {Field,Op,Val} when is_atom(Field)->
        RecordDef = new_record(Name),
        Type = estore_utils:get_value('type',get_value(Field,RecordDef),undefined),
        value_to_string(Field) ++ " " ++  
        value_to_string(Op) ++ " " ++  
        format_to_sql(Type,Val);
      AndEtc when is_atom(AndEtc) ->
        value_to_string(AndEtc);
      {{Y,M,D},undefined} ->
        format_to_sql({'date',[]},{Y,M,D});
      {undefined,{H,M,S}} ->
        format_to_sql({'time',[]},{H,M,S});
      {{Y,Mt,D},{H,M,S}} ->
        format_to_sql({'timestamp',[]},{{Y,Mt,D},{H,M,S}})
      end ++ " "
  end,[],WhereList),
  " WHERE " ++ strip_comma(Where).

select_to_record(Name,Cols,Vals) when is_list(Vals) ->
  lists:foldl(fun(Tuple,Acc) -> Acc ++ select_to_record(Name,Cols,Tuple) end,[],Vals);  

select_to_record(Name,Cols,Tuple) when is_tuple(Tuple) ->
  Fields = [X || {_,X,_,_,_,_} <- Cols],
  Vals = tuple_to_list(Tuple),
  PropList = lists:zip(Fields,Vals),
  results_to_record(new_model(Name),new_record(Name),fields(Name),PropList).

results_to_record(Record,RecordDef,[Field|Fields],PropList) ->
  FieldBin = atom_to_binary(Field,'utf8'),
  BinVal = estore_utils:get_value(FieldBin,PropList,undefined),
  Type = estore_utils:get_value('type',get_value(Field,RecordDef),undefined),
  Val = convert_to_result(Type,BinVal),
  NewRecord = set_value(Field,Val,Record),
  results_to_record(NewRecord,RecordDef,Fields,PropList);
results_to_record(Record,_RecordDef,[],_PropList) ->
  Record.


%% -----------------------------------------------------------------------------
%% ------------------------------ DELETE ---------------------------------------
%% -----------------------------------------------------------------------------

delete_sql(Name,Id) when is_integer(Id) ->
  delete_sql(Name,[{'id','=',Id}]);

delete_sql(Name,Where) when is_list(Where) ->
  "DELETE FROM " ++ table_name(Name) ++ where(Name,Where).

%% -----------------------------------------------------------------------------
%% ------------------------------ INDEXES --------------------------------------
%% -----------------------------------------------------------------------------
 
create_index(Table,Field) when is_atom(Field) ->
  Name = index_name(Table,Field),
  case ?SQUERY(create_index_sql(Table,Field)) of
    {ok,_,_} -> {ok,Name};
    Error -> {error,Error}
  end;
create_index(Table,Fields) when is_list(Fields) ->
  lists:foldl(fun(Field,Acc) -> 
    Acc ++ [create_index(Table,Field)] end
  ,[],Fields). 

create_index_sql(Table,Column) ->
  Name = index_name(Table,Column),
  "DO $$ BEGIN IF NOT EXISTS ("
    "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relname = '" ++ Name ++ "'" 
    "AND n.nspname = '" ++ value_to_string(?SCHEMA) ++ "') THEN "
    "CREATE INDEX " ++ Name ++ " ON " ++ table_name(Table)
    ++ " (" ++ value_to_string(Column) ++ ");"
  "END IF; END$$;".

drop_index(Table,Column) ->
  drop_index(index_name(Table,Column)).

drop_index(Name) ->
  case ?SQUERY(sql_drop_index(Name)) of
    {ok,_,_} -> {ok,Name};
    Error -> {error,Error}
  end.

index_name(Table,Column) ->
  "ix_" ++ value_to_string(Table) ++ "_" ++ value_to_string(Column).

sql_drop_index(Name) ->
  "DROP INDEX " ++ table_name(Name) ++ ";".

transaction(Stmt) ->
  sql_transaction(Stmt).

sql_transaction(Stmt) ->
  "BEGIN;\n " ++ Stmt ++ "COMMIT;\n".

rollback() ->
  sql_rollback().

sql_rollback() ->
  "ROLLBACK;".


%% ------------------------------------------------------------------------------


%% -----------------------------------------------------------------------------
%% --------------------------- CONVERTERS --------------------------------------
%% -----------------------------------------------------------------------------

convert_to_result(_Type,'null') ->
  'null';
convert_to_result({'integer',_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({'bigserial',_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({'bigint',_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({'serial',_Opts},Val) ->
  binary_to_integer(Val);
convert_to_result({Decimal,_Opts},Val) when
           Decimal =:= 'decimal'
    orelse Decimal =:= 'float'
    orelse Decimal =:= 'numeric' ->
  estore_utils:bin_to_num(Val);
convert_to_result({'varchar',_Opts},Val) ->
  binary_to_list(Val);
convert_to_result({'date',_Opts},Val) ->
  estore_utils:date_to_erlang(Val,'iso8601');
convert_to_result({'time',_Opts},Val) ->
  estore_utils:time_to_erlang(Val,'iso8601');
convert_to_result({'timestamp',_Opts},Val) ->
  estore_utils:datetime_to_erlang(Val,'iso8601');
convert_to_result({'null',_Opts},_Val) ->
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
format_to_sql({Decimal,Opts},Value) when 
           Decimal =:= 'decimal' 
    orelse Decimal =:= 'float' 
    orelse Decimal =:= 'numeric' ->
  Decimals = estore_utils:get_value(scale,Opts,0),
  value_to_string({Value,Decimals});
format_to_sql({'time',_Opts},Value) ->
  "'" ++ estore_utils:format_time(Value,'iso8601') ++ "'";
format_to_sql({'date',_Opts},Value) ->
  "'" ++ estore_utils:format_date(Value,'iso8601') ++ "'";
format_to_sql({'timestamp',_Opts},Value) ->
  "'" ++ estore_utils:format_datetime(Value,'iso8601') ++ "'";
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

options_to_string({constraints,{_T,_F}},{references,Opts}) ->
  Ref = estore_utils:get_value('table',Opts,undefined),
  OnDelete = estore_utils:get_value('on_delete',Opts,undefined),
  OnUpdate = estore_utils:get_value('on_update',Opts,undefined),
  "REFERENCES " ++ table_name(Ref) ++ " (id)" ++
  if OnDelete =:= undefined -> ""; true -> 
    " ON DELETE " ++ value_to_string(OnDelete) ++ "" end ++ 
  if OnUpdate =:= undefined -> ""; true -> 
    " ON UPDATE " ++ value_to_string(OnUpdate) ++ "" end
  ++ " ";
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


options_to_string('varchar',Opts) ->
  Length = estore_utils:get_value('length',Opts,undefined),
  if Length =:= undefined -> ""; true ->
   "(" ++  value_to_string(Length) ++ ")" end;
options_to_string('bigint',_Opts) -> 
  "";
options_to_string('integer',_Opts) -> 
  "";
options_to_string(Decimal,Opts) when
           Decimal =:= 'decimal'
    orelse Decimal =:= 'float'
    orelse Decimal =:= 'numeric' -> 
  Precision = estore_utils:get_value(precision,Opts,undefined),
  Scale = estore_utils:get_value(scale,Opts,undefined),
  if Precision =:= undefined andalso Scale =:= undefined -> ""; true ->
    if Precision /= undefined andalso Scale =:= undefined -> 
      "(" ++ value_to_string(Precision) ++ ")";
    true -> 
      if Precision /= undefined andalso Scale /= undefined ->
        "(" ++ value_to_string(Precision) ++ ", " ++ value_to_string(Precision) ++ ")";
      true -> 
        ""
      end
    end  
  end;
options_to_string('time',_Opts) -> 
  "";
options_to_string('date',_Opts) -> 
  "";
options_to_string('timestamp',_Opts) ->
  "";
options_to_string('bigserial',_Opts) ->
  "";
options_to_string('serial',_Opts) ->
  "";
options_to_string('type',{Type,FldOpts}) ->
  value_to_string(Type) ++ " " ++ options_to_string(Type,FldOpts);
options_to_string(_Key,undefined) ->
  [].

value_to_string(V) when is_atom(V) andalso V /= undefined ->
  atom_to_list(V); 
value_to_string({V,Dec}) when is_float(V) ->
  float_to_list(V,Dec);
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

table_name(Name) ->
  has_value(schema,?SCHEMA) ++ value_to_string(Name). 

strip_comma(String) ->
  string:strip(string:strip(String,both,$ ),both,$,).


