-record(model,{name,schema,fields}).
-record(field,{name,type,length,null,position}).
-record(unique,{id,fields}).
-record(pk,{id,fields}).
-record(fk,{id,on_delete_cascade,fields,r_schema,r_table,r_fields}).

