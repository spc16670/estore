-module(estore_es).

-export([
  init/0
  ,new/1
  ,models/0
  ,save/1
  ,delete/1
  ,delete/2 
  ,find/2
  ,find/5
]).

-export([
  req/4
  ,req/7
  ,type_urls/0
  ,type_url/1
  ,record_meta/1

  %%--
  ,insert/0
  %%--

  ,search/2
  ,search/5
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
  ,bulk_insert_records/1
  ,bulk_insert_record/1
]).

-include("$RECORDS_PATH/es.hrl").

-record('resp',{result,response,code,headers,calltime}).

-compile({parse_transform,estore_dynarec}).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

new(Name) ->
  new_model(Name).

save(Record) ->
  bulk_insert_record(Record).

delete(Record) ->
  Name = estore_utils:record_name(Record),
  Id = get_value(id,Record),
  delete(Name,Id).

delete(Name,Id) ->
  case type_url(Name) of
    {ok,{Name,{Url,Type}}} ->
      IdBin = ensure_binary(Id),
      TypeId = <<Type/binary,<<"/">>/binary,IdBin/binary>>,
      req('delete',Url,TypeId,<<>>);
    {error,ErrorTuple} -> 
      {error,ErrorTuple}
  end.

find(Name,Id) when is_integer(Id) ->
  search(Name,Id);
find(Name,Conditions) when is_list(Conditions) ->
  search(Name,Conditions,[],50,0).

find(Name,Where,OrderBy,Limit,Offset) when is_list(Where) ->
  search(Name,Where,OrderBy,Limit,Offset).

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
req(Method,IndexUrl,Type,Data) ->
  EsHost = estore_utils:get_db_config(es,es_url,[]),
  Timeout = estore_utils:get_db_config(es,timeout,infinity), 
  Url = EsHost ++ "/" ++ filename:join([binary_to_list(IndexUrl),binary_to_list(Type)]),
  Hdrs = [],
  Opts = [{recv_timeout, Timeout}, {connect_timeout, Timeout}],
  Track = [],
  Tag = [], 
  req(Method,list_to_binary(Url),Hdrs,Data,Opts,Tag,Track).

%% lhttpc 
%%
%% Proxy in options: 
%%   [{proxy,"http://proxy.com}]
%% Basic auth in headers: 
%%   [{"Authorization","Basic " ++ binary_to_list(base64:encode(U ++":"++ P))}]
%%
%% lhttpc does not perform so well. For some reason sometimes it seems to
%% be falling back to doing GETs even though a different method is specified.
%%
%req(Method,Url,Hdrs,Data,Opts,Timeout,_Tag,_Track) ->
%  io:fwrite("DEBUG: ~p ~p ~p ~p ~p ~n",[Method,Url,Hdrs,Data,Opts]),
%  {Time,Result} = timer:tc(lhttpc,request,[Url,Method,Hdrs,Data,Timeout,Opts]),
%  Resp = case Result of
%    {ok,{{Status,_RStr},RespHdrs,RespBody}} when Status =:= 200; Status =:= 201 ->
%      #'resp'{'result'=ok,'code'=Status,'headers'=RespHdrs,'response'=RespBody};
%    {ok,{{Status,_RStr},RespHdrs,RespBody}} ->
%      #'resp'{'result'=ok,'code'=Status,'headers'=RespHdrs,'response'=RespBody};
%    {error,timeout} ->
%      #'resp'{'result'=timeout};
%    {error,StackTrace} ->
%      #'resp'{'result'=error,'response'=StackTrace}
%  end,
%  CallTime = estore_utils:format_calltime(Time),
%  Resp#'resp'{'calltime'=CallTime}.

req(Method,Url,Hdrs,Data,Opts,_Tag,_Track) ->

  io:fwrite("DEBUG: ~p ~p ~p ~p ~p ~n",[Method,Url,Hdrs,Data,Opts]),

  {Time,Result} = timer:tc(hackney,request,[Method,Url,Hdrs,Data,Opts]),
  Resp = case Result of
    {ok,Status,RespHdrs,Client} when Status =:= 200; Status =:= 201 ->
      case hackney:body(Client) of
	{ok, RespBody} ->
          #'resp'{'result'=ok,'code'=Status,'headers'=RespHdrs,'response'=jsx:decode(RespBody)};
	{error, Reason} ->
          #'resp'{'result'=error,'code'=Status,'headers'=RespHdrs,'response'=Reason}
      end;
    {ok,Status,RespHdrs,Client} ->
      case hackney:body(Client) of
	{ok, RespBody} -> 
	  #'resp'{'result'=ok,'code'=Status,'headers'=RespHdrs,'response'=jsx:decode(RespBody)};
	{error, Reason} -> 
          #'resp'{'result'=error,'code'=Status,'headers'=RespHdrs,'response'=Reason}
      end;
    {error,Reason} ->
      #'resp'{'result'=error,'response'=Reason}
  end,
  CallTime = estore_utils:format_calltime(Time),
  Resp#'resp'{'calltime'=CallTime}.

%% ----------------------------------------------------------------------------
%% ------------------------ INDEXES AND MAPPINGS ------------------------------
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
      req('get',IndexUrl,<<"_mapping">>,Json)
  end.

delete_index(Name) ->
  case mapping_json(Name) of
    {error,Error} -> {error,Error};
    {ok,{Name,PropLst}} ->
      IndexUrl = estore_utils:get_value(url,PropLst,[]),
      req('delete',IndexUrl,<<>>,<<>>)
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
      req('put',IndexUrl,<<>>,Json)
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
    {ok,{Name,{list_to_binary(IndexUrl),list_to_binary(Type)}}}
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

record_meta(Record) ->
  Name = estore_utils:record_name(Record),
  Kv = estore_json:record_to_kv(Record),
  PropKv = estore_utils:get_value(<<"data">>,Kv,[]), 
  case type_url(Name) of
    {ok,{Name,{Url,Type}}} ->
      {ok,{Name,[
        {json,jsx:encode(PropKv)}
        ,{type,Type}
        ,{url,Url}
      ]}};
    Error -> Error
  end.
 
%% ----------------------------------------------------------------------------
%% ---------------------------- BULK INSERT -----------------------------------
%% ----------------------------------------------------------------------------

insert() ->
  Records = estore_test:test(estore_es),
  bulk_insert_records(Records).

bulk_insert_records(Records) ->
  lists:foldl(fun(R,Acc) ->
    Acc ++ [bulk_insert_record(R)]
  end,[],Records).
   
bulk_insert_record(Record) ->
  {ok,{_Name,MetaKv}} = record_meta(Record),
  Url = estore_utils:get_value(url,MetaKv,<<>>),
  Type = estore_utils:get_value(type,MetaKv,<<>>),
  Json = estore_utils:get_value(json,MetaKv,<<>>),
  HasId = case get_value(id,Record) of
    undefined -> [];
    Id -> [{'_id',Id}]
  end, 
  IndexData = jsx:encode([{'index',[{'_index',Url},{'_type',Type}] ++ HasId}]),
  Post = <<IndexData/binary,$\n,Json/binary,$\n>>,
  req('post',<<"_bulk">>,<<>>,Post).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

search(Name,Id) ->
  case type_url(Name) of
    {ok,{Name,{Url,Type}}} ->
      IdBin = ensure_binary(Id),
      TypeId = <<Type/binary,<<"/">>/binary,IdBin/binary>>,
      Resp = req('get',Url,TypeId,<<>>),
      SrcKv = estore_utils:get_value(<<"_source">>,Resp#'resp'.'response',[]),
      estore_json:json_to_record(estore_json:kv_wrapper(Name,SrcKv));
    {error,ErrorTuple} -> 
      {error,ErrorTuple}
  end.

search(_Name,_Where,_OrderBy,_Limit,_Offset) ->
  ok.

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

ensure_binary(Val) when is_float(Val) ->
  estore_utils:num_to_bin(Val);
ensure_binary(Val) when is_integer(Val) ->
  estore_utils:num_to_bin(Val);
ensure_binary(Val) ->
  case io_lib:printable_list(Val) of
    true -> list_to_binary(Val);
    false -> <<>>
  end.




