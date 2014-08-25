-record(address_type,{
  type = [
    {type,{varchar,[{length,50}]}}
  ]  
}).
 
-record(address,{
  line1 = [
    {type,{varchar,[{length,50}]}}
  ]
  ,line2 = [
     {type,{varchar,[{length,50}]}}
  ]
  ,line3 = [
     {type,{varchar,[{length,50}]}}
  ]
  ,postcode = [
    {type,{varchar,[{length,50}]}}
  ]
  ,city = [
    {type,{varchar,[{length,50}]}}
  ]
  ,country = [
    {type,{varchar,[{length,50}]}}
  ]
  ,type = [
    {constraints,[
      {one_to_one,address_type}
    ]}
  ]
}).

-record(phone_type,{
  type = [
    {type,{varchar,[{length,50}]}}
  ]  
}).

-record(phone, {
  number = [
    {type,{varchar,[{length,50}]}}
  ]
  ,type = [
    {constraints,[
      {one_to_one,phone_type}
    ]}
  ]  
}).

-record(shopper,{
  fname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,mname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,lname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,dob = [
    {type,date}
  ]
  ,phone = [
    {constraints,[
    %% one_to_many creates a lookup table
      {one_to_many,phone}
      %%,{on_delete,cascade}
    ]}
  ]
  ,address = [
    {constraints,[
      %% one_to_many creates a lookup table
      {one_to_many,address}
      %%,{on_delete,cascade}
    ]}
  ]
}).

-record(user,{
  email = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,false}]}
  ]
  ,password = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,false}]}
  ]
  ,shopper = [
    {constraints,[
      %% one_to_one references the id of the other model
      {one_to_one,shopper}
      %%,{on_delete,cascade}
      %%,{on_update,cascade}
    ]}
  ]
}).


