-module(estore_json).

-export([
  json_to_record/2
]).

-include("$RECORDS_PATH/estore.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

json_to_record(Name,Json) ->
  KVList = jsx:decode(Json),
  RecordDef = new_record(Name),
  DbModule = estore_utils:get_module(esotre_utils:record_origin(Name)),
  json_to_record(new_record(Name),RecordDef,fields(Name),KVList,DbModule).

json_to_record(Record,RecordDef,[Field|Fields],PropList,DbModule) ->
  FieldBin = atom_to_binary(Field,'utf8'),
  BinVal = estore_utils:get_value(FieldBin,PropList,undefined),
  Type = estore_utils:get_value('type',get_value(Field,RecordDef),undefined),
  Val = DbModule:convert_to_result(Type,BinVal),
  NewRecord = set_value(Field,Val,Record),
  json_to_record(NewRecord,RecordDef,Fields,PropList,DbModule);
json_to_record(Record,_RecordDef,[],_PropList,_DbModule) ->
  Record. 



