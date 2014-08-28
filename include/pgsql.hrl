-record(address_type,{
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,type = [
    {type,{varchar,[{length,50}]}}
  ]  
}).
 
-record(address,{
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,line1 = [
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
    s_null(Record,FieldName) ->
{constraints,[
      {references,address_type}
      ,{null,false}
    ]}
  ]
}).

-record(phone_type,{
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,type = [
    {type,{varchar,[{length,50}]}}
  ]  
}).

-record(phone, {
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,number = [
    {type,{varchar,[{length,50}]}}
  ]
  ,type = [
    {constraints,[
      {references,phone_type}
      ,{null,false}
    ]}
  ]  
}).

-record(shopper,{
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,fname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,mname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,lname = [
    {type,{varchar,[{length,50}]}}
  ]
  ,dob = [
    {type,{date,[{format,[]}]}}
  ]
  ,phone = [
    {constraints,[
    %% one_to_many creates a lookup table
      {one_to_many,phone}
      ,{null,true}
      %%,{on_delete,cascade}
    ]}
  ]
  ,address = [
    {constraints,[
      %% one_to_many creates a lookup table
      {one_to_many,address}
      ,{null,true}
      %%,{on_delete,cascade}
    ]}
  ]
}).

-record(user,{
  id = [ 
    {type,{bigserial,[]}}
  ]
  ,email = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,false}]}
  ]
  ,password = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,false}]}
  ]
  ,shopper = [
    {type,{'bigint',[]}}
    ,{constraints,[
      %% references references the id of the other model
      {references,shopper}
      ,{null,true} 
      %%,{on_delete,cascade}
      %%,{on_update,cascade}
    ]}
  ]
}).


