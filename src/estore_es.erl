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
  ,create_indexes/0
  ,create_index/1
  ,get_mappings/0
  ,get_mapping/1
  ,delete_index/1
  ,index_exists/1
  ,ensure_indexes_exist/0
  ,mappings_json/0
  ,mapping_json/1
  ,mapping_json/3
]).

-include("$RECORDS_PATH/es.hrl").

-record('resp',{result,response,code,headers,calltime}).

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
  Indices = ensure_indexes_exist(),
  {ok,Indices}.

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
  Url = EsHost ++ "/" ++ filename:join([IndexUrl,Type]),
  Hdrs = [],
  Opts = [],
  Track = [],
  Tag = [],
  Timeout = estore_utils:get_db_config(es,timeout,infinity), 
  req(Method,Url,Hdrs,Data,Opts,Timeout,Tag,Track).

req(Method,Url,Hdrs,Data,Opts,Timeout,_Tag,_Track) ->
  {Time,Result} = timer:tc(lhttpc,request,[Url,Method,Hdrs,Data,Timeout,Opts]),
  Resp = case Result of
    {ok,{{HttpCode,_RespStr},RespHdrs,RespBody}} ->
      #'resp'{'result'=ok,'code'=HttpCode,'headers'=RespHdrs,'response'=RespBody};
    {error,timeout} ->
      #'resp'{'result'=timeout};
    {error,StackTrace} ->
      #'resp'{'result'=error,'response'=StackTrace}
  end,
  CallTime = estore_utils:format_calltime(Time),
  Resp#'resp'{'calltime'=CallTime}.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

ensure_indexes_exist() ->
  lists:foldl(fun(E,Acc) ->
    R = case index_exists(E) of
      true -> {exists,E};
      false -> create_index(E)
    end,
    Acc ++ [R]
  end,[],records()).  

index_exists(Name) ->
  Resp = get_mapping(Name),
  if Resp#'resp'.'result' =:= ok andalso Resp#'resp'.'code' =:= 404 -> 
  false; true -> true end.

get_mappings() ->
  lists:foldl(fun(E,Acc) ->
    Acc ++ [get_mapping(E)]
  end,[],records()).

get_mapping(Name) ->
  case mapping_json(Name) of
    {error,Error} -> {error,Error};
    {ok,{Name,PropLst}} ->
      IndexUrl = estore_utils:get_value(url,PropLst,[]),
      Json = estore_utils:get_value(json,PropLst,[]),
      req('get',IndexUrl,"_mapping",Json)
  end.

delete_index(Name) ->
  case mapping_json(Name) of
    {error,Error} -> {error,Error};
    {ok,{Name,PropLst}} ->
      IndexUrl = estore_utils:get_value(url,PropLst,[]),
      req('delete',IndexUrl,[],[])
  end.

create_indexes() ->
  lists:foldl(fun(E,Acc) ->
    Acc ++ [create_index(E)]
  end,[],records()).

create_index(Name) ->
  case mapping_json(Name) of
    {error,Error} -> {error,Error};
    {ok,{Name,PropLst}} ->
      IndexUrl = estore_utils:get_value(url,PropLst,[]),
      Json = estore_utils:get_value(json,PropLst,[]),
      req('put',IndexUrl,[],Json)
  end.

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
      {error,ErrorTuple}
  end.

mapping_json(Name,Url,Type) ->
  Record = new_record(Name),
  Kv = estore_json:record_to_kv(Record),
  PropKv = estore_utils:get_value(<<"data">>,Kv,[]), 
  MappingKv = mapping_struct(Type,PropKv), 
  {ok,{Name,[
    {json,jsx:encode(MappingKv)}
    ,{type,Type}
    ,{url,Url}
  ]}}.

mapping_struct(Type,PropKv) ->
  TypeBin = estore_json:erlang_to_json(Type), 
  [{mappings,[
    {TypeBin,[
      {properties,PropKv}
    ]}
  ]}].

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------













