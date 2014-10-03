-module(mnesia_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("mnesia.hrl").

-define(TEST_MODULE,mnesia).

begin_test() ->
  ok = application:start(estore).

%% -----------------------------------------------------------------------------
%% ----------------------------- MODELS ----------------------------------------
%% -----------------------------------------------------------------------------

models_test() ->
  records(estore:models(?TEST_MODULE)).

records([Record|Records]) ->
  ?assert(is_tuple(Record)),
  records(Records);
records([]) ->
  done.

%% -----------------------------------------------------------------------------
%% ------------------------------ NEW ------------------------------------------
%% -----------------------------------------------------------------------------

new_test() ->
  RecordNames = lists:foldl(fun(Record,Acc) -> 
    Acc ++ [hd(tuple_to_list(Record))]
  end,[],estore:models(?TEST_MODULE)),
  new(RecordNames).
   
new([Name|Names]) ->
  ?assert(is_tuple(estore:new(?TEST_MODULE,Name))),
  new(Names);
new([]) ->
  done.

%% ----------------------------------------------------------------------------
%% ------------------------------- INIT ---------------------------------------
%% ----------------------------------------------------------------------------

init_test() ->
  ?assertMatch({ok,_},estore:init(?TEST_MODULE)).

%% ----------------------------------------------------------------------------
%% ------------------------------- SAVE ---------------------------------------
%% ----------------------------------------------------------------------------

save_user_visits_test() ->
  UserVisitRecord = estore:new(?TEST_MODULE,user_visits),
  UserVisits = [
    UserVisitRecord#'user_visits'{
      key="french@mustard.fr", visits=20, reviews_given=1, purchases=0}
    ,UserVisitRecord#'user_visits'{
      key="turkish@kebab.de", visits=15, reviews_given=15, purchases=15}
    ,UserVisitRecord#'user_visits'{
      key="sousage@roll.co.uk", visits=104, reviews_given=2, purchases=15}
    ,UserVisitRecord#'user_visits'{
      key="rumcajs@rozbojnik.pl", visits=50, reviews_given=15, purchases=5}
  ],
  Result = estore:save(?TEST_MODULE,UserVisits),
  ?assertMatch({ok,_},Result).
  
%% ----------------------------------------------------------------------------
%% ------------------------------- FIND ---------------------------------------
%% ----------------------------------------------------------------------------

find_stats_test() -> 
  ?assert(is_list(estore:find(?TEST_MODULE,user_visits,"french@mustard.fr"))).

%% ----------------------------------------------------------------------------
%% ------------------------------ DELETE --------------------------------------
%% ----------------------------------------------------------------------------

%% ----------------------------------------------------------------------------

end_test() ->
  application:stop(estore).

