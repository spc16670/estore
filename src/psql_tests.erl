-module(psql_tests).

-export([
  run/0
  ,print/2
]).

-include("psql.hrl").

-define(SCHEMA,lamazone).
-define(TABLE,shopper).
-define(TABLE2,test_table2).
-define(DB,test_db).
-define(FIELD,field).
-define(FIELD2,field2).
-define(FIELDS,[{'F',field},{'F2',field2},?FIELD]).
-define(INDEX,sample_index).
-define(WHERE,[{{t,oid},'=',{ix,indexrelid}},'AND',{{i,oid},'=',{ix,indexrelid}}]).
-define(GROUPBY,?FIELDS).
-define(JOIN,[{{'LEFT OUTER JOIN',{?SCHEMA,?TABLE2,'T2'}},'ON',{{'T',?TABLE},'=',{'T2',?TABLE2}}}]).

%% -----------------------------------------------------------------------------

run() ->
  print('create_schema/1',psql:create_schema(?SCHEMA)),
  print('create_schema/2',psql:create_schema(?SCHEMA,['ifnotexists'])),
  print('create_index/3',psql:create_index(someindex,sometab,[col1,col2])),
  print('create_index/4',psql:create_index(someindex,{schema,sometab},[col1,col2],[nolock])),
  print('drop_table/1',psql:drop_table(?TABLE)),
  print('drop_table/2',psql:drop_table(?SCHEMA,?TABLE)),
  print('drop_table/3',psql:drop_table(?SCHEMA,?TABLE,[ifexists,cascade])),

  print('select/1 {S,T,A}',psql:select({?SCHEMA,?TABLE,'T'})),
  print('select/1 {S,T}',psql:select({?SCHEMA,?TABLE})),
  print('select/1 T',psql:select(?TABLE)),
  print('select/1 [{S,T,A},{S,T,A}]',psql:select([{?SCHEMA,?TABLE,'T'},{?SCHEMA,?TABLE2,'T2'}])),
  print('select/2 {S,T,A}, field',psql:select({?SCHEMA,?TABLE,'T'},field)),
  print('select/2 {S,T}, field',psql:select({?SCHEMA,?TABLE},field)),
  print('select/2 T, field',psql:select({?SCHEMA,?TABLE},field)),
  print('select/2 [{S,T,A},{S,T,A}], field',psql:select([{?SCHEMA,?TABLE,'T'},{?SCHEMA,?TABLE2,'T2'}],field)),
  print('select/3 {S,T,A},field,W',psql:select({?SCHEMA,?TABLE,'T'},field,?WHERE)),
  print('select/3 {S,T},field,W',psql:select({?SCHEMA,?TABLE},field,?WHERE)),
  print('select/3 T,field,W',psql:select(?TABLE,field,?WHERE)),
  print('select/3 [{S,T,A},{S,T,A}],[fields],W',psql:select([{?SCHEMA,?TABLE,'T'},{?SCHEMA,?TABLE2,'T2'}],?FIELDS,?WHERE)),
  print('select/4 {S,T,A},field,W,G',psql:select({?SCHEMA,?TABLE,'T'},field,?WHERE,?GROUPBY)),
  print('select/4 {S,T},field,W,G',psql:select({?SCHEMA,?TABLE},field,?WHERE,?GROUPBY)),
  print('select/4 T,field,W,G',psql:select(?TABLE,field,?WHERE,?GROUPBY)),
  print('select/4 [{S,T,A},{S,T,A}],field,W,G',psql:select([{?SCHEMA,?TABLE,'T'},{?SCHEMA,?TABLE2,'T2'}],field,?WHERE,?GROUPBY)),
  print('select/5 {S,T,A},field,J,W,G',psql:select({?SCHEMA,?TABLE,'T'},field,?JOIN,?WHERE,?GROUPBY)),
  print('select/5 {S,T},field,J,W,G',psql:select({?SCHEMA,?TABLE},field,?JOIN,?WHERE,?GROUPBY)),
  print('select/5 T,field,J,W,G',psql:select(?TABLE,field,?JOIN,?WHERE,?GROUPBY)),
  print('select/5 [{S,T,A},{S,T,A}],field,J,W,G',psql:select([{?SCHEMA,?TABLE,'T'},{?SCHEMA,?TABLE2,'T2'}],field,?JOIN,?WHERE,?GROUPBY)),

  print('create_table/2 table,[field]',psql:create_table(?TABLE,[?FIELD])),
  print('create_table/3 schema,table,[field]',psql:create_table(?SCHEMA,?TABLE,[?FIELD])),
  Constraints = [
    #fk{id=sample_id,on_delete_cascade=yes,fields=[?FIELD],r_schema=?SCHEMA,r_table=?TABLE2,r_fields=[?FIELD2]}
    ,#pk{id=sample_id,fields=[?FIELD2]}
    ,#unique{id=sample_id,fields=[?FIELD2]}
  ],
  print('create_table/4 schema,table,[field],constraints',psql:create_table(?SCHEMA,?TABLE,[?FIELD],Constraints)),
  print('select_index/1 index',psql:select_index(?INDEX)),
  print('select_index/2 schema,index',psql:select_index(?SCHEMA,?INDEX)),
  print('table_info/3 db,schema,tb',psql:table_info(?DB,?SCHEMA,?TABLE)).

print(T,R) ->
  io:fwrite("~n~n" ++ atom_to_list(T) ++ "~n~s",[R]).

