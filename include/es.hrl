%% Some information on ES types can be found here:
%% http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping.html
%%
%% dates:
%% http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-date-format.html
%% For custom dates
%% http://joda-time.sourceforge.net/api-release/org/joda/time/format/DateTimeFormat.html
%%
%% ES record names must be prefixed with the name of the index 

-record(kfis_staff,{
  fname = [{type,'string'}]
  ,lname = [{type,'string'}]
  ,dob = [
    {type,'date'}
    ,{format,'basic_date'}
  ]
  ,age = [{type,'integer'}]
}).
