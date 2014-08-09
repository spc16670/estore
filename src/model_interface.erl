-module(model_interface).
 
-callback save(Name :: list(), Map :: map()) -> 
  'ok'|tuple('error', Reason :: string()).

-callback delete(Name :: list(), Map :: map()) -> 
  'ok'|tuple('error', Reason :: string()).

-callback find(Name :: list(), Conditions :: list()) -> 
  'ok'|tuple('error', Reason :: string()).

