-module(estore_pgsql).

-export([
  create_table/2
  ,create_index/3
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
  create_schema/1
  ,drop_tables/0
  ,create_tables/1
  ,one_to_many/2
]).


-export([
  squery/2
  ,equery/3
]).

-include("estore.hrl").
-include("pgsql.hrl").

-compile({parse_transform,estore_dynarec}).

-define(SCHEMA,estore:get_db_config(pgsql,tablespace)).

%% -----------------------------------------------------------------------------

new(Name) ->
 new_model(Name).

save(_Record) ->  
  ok.

delete(_Name) ->
  ok.

find(_Name,_Conditions) ->
  ok.

init() ->
  drop_tables(),
  create_schema(?SCHEMA),
  create_tables(models()).

models() ->
  lists:foldl(fun(E,Acc) ->
     Acc ++ [new_record(E)]
  end,[],records()).

squery(PoolName, Sql) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {squery, Sql})
  end).

equery(PoolName, Stmt, Params) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {equery, Stmt, Params})
  end). 

%% -----------------------------------------------------------------------------

new_model(Name) ->
  new_model(fields(Name),new_record(Name)).

new_model([F|Fs],Record) ->
  R = set_value(F,undefined,Record),
  new_model(Fs,R);
new_model([],Record) ->
  Record.


%% -----------------------------------------------------------------------------

create_schema(undefined) ->
  ok;
create_schema(S) ->
  create_schema(S,[{ifexists,false}]).
create_schema(S,Op) ->
  "CREATE SCHEMA " ++ options_to_string({options,ifexists},Op) 
  ++ " " ++ value_to_string(S) ++ ";".

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

create_tables(Records) ->
  lists:foldl(fun(E,Acc) ->
    Acc ++ [create_table(E)] 
  end,[],Records). 

create_table(Record) ->
  create_table(Record,[{ifexists,false}]).

create_table(Record,Options) ->
  Name = hd(tuple_to_list(Record)),
  Fields = string:strip(string:strip(convert_fields(Name),both,$ ),both,$,),
  create_table(Name,Fields,Options).
create_table(Name,Fields,Options) ->
  "CREATE TABLE " ++ options_to_string({options,ifexists},Options) ++ " " 
  ++ has_value(schema,?SCHEMA) ++ value_to_string(Name) 
  ++ " (" ++ Fields ++ "\n);".

  %% Stmt ++  "\n" ++ add_constraint(Schema,Name,Constraints),
  %% transaction(Stmt).

%% -----------------------------------------------------------------------------

convert_fields(Name) ->
  lists:foldl(fun(E,Acc) -> 
    FldTuple = {{Name,E},get_value(E,new_record(Name))},
    Acc ++ convert_field(FldTuple)
  end,[],fields(Name)).
  
convert_field({{Table,Field},FldOpts}) ->
  value_to_string(Field) ++ " " ++ 
  string:strip(options_to_string({field,{Table,Field}},FldOpts),both,$ ) ++ ", ".

options_to_string({field,{T,F}},FldOpts) ->
  options_to_string(type,proplists:get_value(type,FldOpts)) ++ " " ++
  options_to_string({constraints,{T,F}},proplists:get_value(constraints,FldOpts));

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
options_to_string({options,_},_MissingClouse) ->
  [];

options_to_string({constraints,{T,_F}},{one_to_many,Table}) ->
  one_to_many(T,Table);
options_to_string({constraints,{_T,_F}},{one_to_one,Table}) ->
  "REFERENCES " ++ value_to_string(Table) ++ " (id) ";
options_to_string({constraints,{_T,_F}},{null,false}) ->
  value_to_string('not') ++ " " ++ value_to_string(null)++ " ";
options_to_string({constraints,{_T,_F}},{null,true}) ->
  value_to_string(null) ++ " ";
options_to_string({constraints,{T,F}},undefined) ->
  options_to_string({constraints,{T,F}},{null,true});
options_to_string({constraints,{T,F}},ConstraintsList) ->
  lists:foldl(fun(E,Acc) -> 
    Acc ++ options_to_string({constraints,{T,F}},E)
  end,[],ConstraintsList);

options_to_string(varchar,{length,Size}) ->
   "(" ++  value_to_string(Size) ++ ")";

options_to_string(type,date) ->
  " " ++ value_to_string(date) ++ " ";
options_to_string(type,bigserial) ->
  " " ++ value_to_string(bigserial) ++ " ";
options_to_string(type,{Type,FldOpts}) ->
  value_to_string(Type) ++ " " ++ lists:foldl(fun(E,Acc) -> 
    Acc ++ options_to_string(Type,E)
  end,[],FldOpts);

options_to_string(_Key,undefined) ->
  [].

%% -----------------------------------------------------------------------------

one_to_many(Table,Ref) ->
  LookupTableName = value_to_string(Table) ++ "_" ++ value_to_string(Ref),
  FieldTuples = [
    {{LookupTableName,Table},[
      {type,bigserial}
      ,{constraints,[{one_to_one,Table}]}
    ]}
    ,{{LookupTableName,Ref},[
      {type,bigserial}
      ,{constraints,[{one_to_one,Ref}]}
    ]}
  ],
  FieldsStr = lists:foldl(fun(E,Acc) -> 
    Acc ++ convert_field(E)
  end,[],FieldTuples),  
  Fields = string:strip(string:strip(FieldsStr,both,$ ),both,$,),
  create_table(LookupTableName,Fields,[{ifexists,false}]),
  [].

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

result_to_value({atom,<<"character varying">>}) ->
  varchar;
result_to_value({atom,<<"bigint">>}) ->
  integer;
result_to_value({atom,<<"money">>}) ->
  float;
result_to_value({atom,<<"numeric">>}) ->
  float;
result_to_value({<<"numeric">>,_L,P,S}) ->
  {list_to_integer(binary_to_list(P)),list_to_integer(binary_to_list(S))};
result_to_value({<<"character varying">>,L,_P,_S}) ->
  list_to_integer(binary_to_list(L));
result_to_value({_T,_L,_P,_S}) ->
  undefined;
result_to_value({atom,B}) when is_binary(B) ->
  list_to_atom(string:to_lower(binary_to_list(B)));
result_to_value({atom,L}) when is_list(L) ->
  list_to_atom(L);
result_to_value({atom,A}) when is_atom(A) ->
  A;
result_to_value({atom,B}) when is_binary(B) ->
  list_to_integer(binary_to_list(B));
result_to_value(null) ->
  undefined.

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


  

