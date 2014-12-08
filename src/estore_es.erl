-module(estore_es).

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
  req/4
  ,req/8
  ,type_urls/0
  ,type_url/1

  %%--
  ,mappings_json/0
  ,mapping_json/1
  ,mapping_json/3
]).

-include("$RECORDS_PATH/es.hrl").

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

new(Name) ->
  new_model(Name).

save(_Model) ->
  ok.

delete(_Name,_Id) ->
  ok.

find(_Name,_Conditions) ->
  ok.

find(_Name,_Where,_OrderBy,_Limit,_Offset) -> 
  ok.

init() ->
  HttpRsc = estore_utils:get_db_config(es,http_rsc), 
  HttpRscDeps = estore_utils:get_db_config(es,http_rsc_deps,[]), 
  estore_app:ensure_started(HttpRscDeps ++ [HttpRsc]),
  mappings_json(),
  ok.

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
%%
%% @doc
%% lhttpc 
%%
%% Proxy in options: 
%%   [{proxy,"http://proxy.com}]
%% Basic auth in headers: 
%%   [{"Authorization","Basic " ++ binary_to_list(base64:encode(U ++":"++ P))}]
%%
req(Method,IndexUrl,Type,Data) ->
  EsHost = estore_utils:get_db_config(es,es_url,[]),
  Url = filename:join([EsHost,IndexUrl,Type]),
  Hdrs = [],
  Opts = [],
  Track = [],
  Tag = [],
  Timeout = estore_utils:get_db_config(es,timeout,infinity), 
  req(Method,Url,Hdrs,Data,Opts,Timeout,Tag,Track).

req(Method,Url,Hdrs,Data,Opts,Timeout,_Tag,_Track) ->
  {Time,Result} = timer:tc(lhttpc,request,[Url,Method,Hdrs,Data,Timeout,Opts]),
  Response = case Result of
    {ok,{{200,_RespStr},_RespHeaders,RespBody}} ->
    {ok,RespBody};
  {ok,{{HttpCode,_RespStr},_RespHeaders,_RespBody}} ->
    {error,"ERROR: Unexpected HTTP code " ++ integer_to_list(HttpCode)};
  {error,timeout} ->
    {timeout,"ERROR: HTTP Request timed out."};
  {error,_StackTrace} ->
    {error,"ERROR: HTTP Request failed."}
  end,
  CallTime = estore_utils:format_calltime(Time),
  {CallTime,Response}.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

type_urls() ->
  lists:foldl(fun(E,Acc) ->
    case type_url(E) of
      {ok,UrlTuple} -> Acc ++ [UrlTuple];
      _ -> Acc
    end
  end,[],records()).

type_url(Name) ->
  try
    UrlPath = re:replace(atom_to_list(Name),"_","/",[{return,list}]),
    Type = string:sub_word(UrlPath,string:words(UrlPath,$/),$/),
    IndexUrl = string:sub_string(UrlPath,1,string:rchr(UrlPath,$/) - 1),
    {ok,{Name,{IndexUrl,Type}}}
  catch Error:Reason -> {error,{Name,{Error,Reason}}}
  end.

mappings_json() ->
  lists:foldl(fun({Name,{Url,Type}},Acc) ->
    Json = mapping_json(Name,Url,Type),
    Acc ++ [Json]
  end,[],type_urls()).

mapping_json(Name) ->
  case type_url(Name) of
    {ok,{Name,{Url,Type}}} ->
      mapping_json(Name,Url,Type);
    {error,ErrorTuple} -> 
      ErrorTuple
  end.

mapping_json(Name,Url,Type) ->
  Record = new_record(Name),
  Kv = estore_json:record_to_kv(Record),
  PropKv = estore_utils:get_value(<<"data">>,Kv,[]), 
  MappingKv = mapping_struct(Type,PropKv), 
  {Name,{Url,jsx:encode(MappingKv)}}.

mapping_struct(Type,PropKv) ->
  TypeBin = estore_json:erlang_to_json(Type), 
  [{mappings,[
    {TypeBin,[
      {properties,PropKv}
    ]}
  ]}].














