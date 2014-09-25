-module(pgsql_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("pgsql.hrl").

-define(TEST_MODULE,pgsql).

%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------

begin_test() ->
  ok = application:start(estore).

models_test() ->
  records(estore:models(?TEST_MODULE)).

records([Record|Records]) ->
  ?assert(is_tuple(Record)),
  records(Records);
records([]) ->
  done.

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

%% -----------------------------------------------------------------------------

init_test() ->
  ?assertMatch({ok,_},estore:init(?TEST_MODULE)).

%% -----------------------------------------------------------------------------

save_user_test() ->
  UserRecord = estore:new(?TEST_MODULE,'user'),
  User = UserRecord#'user'{
    'email' = "simonpeter@mail.com"
    ,'password' = "password"
    ,'date_registered' = calendar:local_time()
  },
  ?assertMatch({ok,_},estore:save(?TEST_MODULE,User)).

save_shopper_test() ->
  ShopperRecord = estore:new(?TEST_MODULE,'shopper'),
  Shopper = ShopperRecord#'shopper'{
    'fname' = "Szymon"
    ,'mname' = "Piotr"
    ,'lname' = "Czaja"
    ,'dob' = {{1987,3,1},{0,0,0}}
    ,'user_id' = 1
  },
  ?assertMatch({ok,_},estore:save(?TEST_MODULE,Shopper)).

save_address_types_test() ->
  AddressTypeRecord = estore:new(?TEST_MODULE,'address_type'),
  AddressTypes = [
    AddressTypeRecord#'address_type'{'type' = "Delivery"}
    ,AddressTypeRecord#'address_type'{'type' = "Residential"}
    ,AddressTypeRecord#'address_type'{'type' = "BankCard"}
    ,AddressTypeRecord#'address_type'{'type' = "Supplier"}
  ],
  ?assert(is_list(estore:save(?TEST_MODULE,AddressTypes))).

save_address_test() ->  
  AddressRecord = estore:new(?TEST_MODULE,'shopper_address'),
  Address = AddressRecord#'shopper_address'{
    line1 = "Flat 1/2"
    ,line2 = "56 Cecil St"
    ,postcode = "G128RJ"
    ,city = "Glasgow"
    ,country = "Scotland"
    ,type = 1
    ,shopper_id = 1
  },
  ?assertMatch({ok,_},estore:save(?TEST_MODULE,Address)).

save_phone_types_test() ->
  PhoneTypeRecord = estore:new(?TEST_MODULE,phone_type),
  PhoneType = PhoneTypeRecord#'phone_type'{'type' = "Mobile"},
  ?assertMatch({ok,_},estore:save(?TEST_MODULE,PhoneType)).

save_shopper_phones_test() ->
  PhoneRecord = estore:new(?TEST_MODULE,'shopper_phone'),
  Phone = PhoneRecord#'shopper_phone'{
    'number' = "07871259234"
    ,'type' = 1
    ,'shopper_id' = 1
  },
  ?assertMatch({ok,_},estore:save(?TEST_MODULE,Phone)).
 
%  %% -- SELECT
%  ShopperR = Module:find(shopper,[{'id','=',ShopperId}]),
%  io:fwrite("~p~n",[ShopperR]),
%  Module:find(user,[{'id','=',UserId}]).

end_test() ->
  application:stop(estore).

