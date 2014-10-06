-module(estore_json).

-export([
  json_to_record/1
  ,record_to_json/1
]).

-include("$RECORDS_PATH/estore.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------

%% Number	number()
%% String	string()
%% Boolean	atom()
%% Array	list()
%% Object	proplist()
%% null		atom()

-define(RECORD_TYPE_KEY,<<"type">>).
-define(RECORD_DATA_KEY,<<"data">>).
-define(JSON_SCOPE_KEY,<<"scope">>).
-define(JSON_SCOPE_REQUEST_VALUE,<<"request">>).
-define(JSON_SCOPE_RESPONSE_VALUE,<<"response">>).

%% ----------------------------------------------------------------------------
%% @spec json_to_record(Json::binary()) -> record() | {error,Error::tuple()}.
%% @doc Turns raw JSON into a record if JSON has the keys and values that 
%% follow the convention and the record is defined.
%%
%% The convention is that every JSON to record candidate must define keys 
%% as specified by the ?RECORD_TYPE_KEY and ?RECORD_DATA_KEY macros.
%%
%% The value of the ?RECORD_TYPE_KEY must reference an already defined record 
%% from the .hrl file. 
%%
%% { "scope" : "request"
%%  , "timestamp" : 785468431331321
%%  , "type" : "address"
%%  , "data" : { "key" : "value"} }

json_to_record(Json) ->
  json_to_record(Json,estore_utils:is_proplist(Json)).
  
json_to_record(Json,false) ->
  json_to_record(jsx:decode(Json),true);
json_to_record(Json,true) ->
  NameBin = estore_utils:get_value(?RECORD_TYPE_KEY,Json,undefined),
  KVData = estore_utils:get_value(?RECORD_DATA_KEY,Json,undefined),
  if NameBin /= undefined ->
    if KVData /= undefined ->
      Name = binary_to_atom(NameBin,'utf8'),
      DbModule = estore_utils:get_module(estore_utils:record_origin(Name)),
      Record = estore:new(DbModule,Name),
      RecordDef = new_record(Name),
      Fields = fields(Name),
      json_to_record(Record,RecordDef,Fields,KVData); 
    true -> 
      {error,{KVData,?RECORD_DATA_KEY}}
    end;
  true -> 
    {error,{NameBin,?RECORD_TYPE_KEY}}
  end.

json_to_record(Record,RecordDef,[Field|Fields],PropList) ->
  FieldBin = atom_to_binary(Field,'utf8'),
  BinVal = estore_utils:get_value(FieldBin,PropList,undefined),
  Type = estore_utils:get_value('type',get_value(Field,RecordDef),undefined),
  Val = json_to_erlang(Type,BinVal),
  NewRecord = set_value(Field,Val,Record),
  json_to_record(NewRecord,RecordDef,Fields,PropList);
json_to_record(Record,_RecordDef,[],_PropList) ->
  Record. 

%% ----------------------------------------------------------------------------

json_to_erlang(_TypeDef,Val) when is_binary(Val) ->
  binary_to_list(Val);
json_to_erlang(_TypeDef,Val) when is_integer(Val) ->
  Val;
json_to_erlang(_TypeDef,Val) ->
  Val.

%% ----------------------------------------------------------------------------
%% @spec record_to_json(Record::record()) -> JSON::binary() | {error,Error::tuple()}.
%% @doc Turns a record into a JSON object binary according specifying some
%% mandatory key as adopted by the convetion.

record_to_json(Record) ->
  Name = ?RECORD_NAME(Record),
  DataStruct = record_to_json(fields(Name),Record,[]),
  JsonStruct = [
    {?JSON_SCOPE_KEY,?JSON_SCOPE_RESPONSE_VALUE}
    ,{?RECORD_TYPE_KEY,atom_to_binary(Name,'utf8')}
    ,{?RECORD_DATA_KEY,DataStruct}
  ], 
  jsx:encode(JsonStruct).
  
record_to_json([Field|Fields],Record,Result) ->
  FieldBin = atom_to_binary(Field,'utf8'),
  Value = get_value(Field,Record),
  ValueBin = erlang_to_json(Value),
  Struct = Result ++ [{FieldBin,ValueBin}],
  record_to_json(Fields,Record,Struct);
record_to_json([],_Record,Result) ->
  Result.
  
erlang_to_json(Val) when is_list(Val) ->
  list_to_binary(Val);
erlang_to_json(Val) when is_integer(Val) ->
  Val;
erlang_to_json(Val) when is_float(Val) ->
  Val;
erlang_to_json(Val) when is_atom(Val) ->
  atom_to_binary(Val,'utf8');
erlang_to_json(Val) ->
  Val.


