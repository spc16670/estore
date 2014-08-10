-record(address,{
  line1 = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
  ,line2 = [
     {type,{varchar,[{length,50}]}}
     ,{constraints,[{null,true}]}
  ]
  ,line3 = [
     {type,{varchar,[{length,50}]}}
     ,{constraints,[{null,true}]}
  ]
  ,postcode = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
  ,city = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
  ,country = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
}).

-record(shopper,{
  fname = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
  ,mname = [
    {type,{varchar,[{length,50}]}}
    ,{constraints,[{null,true}]}
  ]
  ,lname = [
    {type,{varchar,[{length,50}]}}
   ,{constraints,[{null,true}]}
  ]
  ,dob = [
    {type,date}
    ,{constraints,[{null,true}]}
  ]
  ,address = [
    {constraints,[
      {one_to_many,address}
      ,{on_delete,cascade}
    ]}
  ]
}).
