-module(basic_SUITE).

-export([
  new/0
  ,new/1
  ,test/0
]).

-include("estore.hrl").

%% -----------------------------------------------------------------------------

new() ->
  new(shopper).
new(Model) ->
  Model:new(Model).

test() ->
  test(estore_pgsql).

test(Module) -> 
  %% -- INIT
  Module:init(),

  %% -- SAVE RECORDS
  UserRecord = estore:new(Module,'user'),
  User = UserRecord#'user'{
    'email' = "simonpeter@mail.com"
    ,'password' = "password"
    ,'date_registered' = calendar:local_time()
  },
  {ok,UserId} = Module:save(User),
  ShopperRecord = estore:new(Module,'shopper'),
  Shopper = ShopperRecord#'shopper'{
    'fname' = "Szymon"
    ,'mname' = "Piotr"
    ,'lname' = "Czaja"
    ,'dob' = {{1987,3,1},{0,0,0}}
    ,'user_id' = UserId
  },
  {ok,ShopperId} = Module:save(Shopper),
  
  AddressTypeRecord = estore:new(Module,'address_type'),
  AddressType = AddressTypeRecord#'address_type'{'type' = "Residential"},
  {ok,AddressTypeId} = Module:save(AddressType),
  
  AddressRecord = estore:new(Module,'shopper_address'),
  Address = AddressRecord#'shopper_address'{
    line1 = "Flat 1/2"
    ,line2 = "56 Cecil St"
    ,postcode = "G128RJ"
    ,city = "Glasgow"
    ,country = "Scotland"
    ,type = AddressTypeId
    ,shopper_id = ShopperId
  },
  {ok,_AddressId} = Module:save(Address),

  PhoneTypeRecord = estore:new(Module,phone_type),
  PhoneType = PhoneTypeRecord#'phone_type'{'type' = "Mobile"},
  {ok,PhoneTypeId} = Module:save(PhoneType),

  PhoneRecord = estore:new(Module,'shopper_phone'),
  Phone = PhoneRecord#'shopper_phone'{
    'number' = "07871259234"
    ,'type' = PhoneTypeId
    ,'shopper_id' = ShopperId
  },
  {ok,_PhoneId} = Module:save(Phone),
 
  %% -- SELECT
  ShopperR = Module:find(shopper,[{'id','=',ShopperId}]),
  io:fwrite("~p~n",[ShopperR]),
  Module:find(user,[{'id','=',UserId}]).


