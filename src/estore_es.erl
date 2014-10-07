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
  estore_app:ensure_started([
    jsx
  ]),
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

