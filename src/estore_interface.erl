-module(estore_interface).

-callback init(Adapter :: atom()) -> 
  'ok'|{'error', Reason :: string()}.

-callback models(Adapter :: atom()) -> 
  Records :: list() | {'error', Reason :: string()}.
 
-callback new(Adapter :: atom(),Record :: any()) -> 
  'ok'|{'error', Reason :: string()}.
 
-callback save(Adapter :: atom(),Record :: any()) -> 
  {'ok', Id :: integer()}|{'error', Reason :: string()}.

-callback delete(Adapter :: atom(),Record :: any()) -> 
  {'ok',Count :: integer()}|{'error', Reason :: string()}.

-callback delete(Adapter :: atom(),Record :: any(),Conditions :: list()) -> 
  {'ok',Count :: integer()}|{'error', Reason :: string()}.

-callback find(Adapter :: atom(),Name :: atom(),Conditions :: list()) -> 
  Records :: list() | {'error', Reason :: string()}.

-callback find(Adapter :: atom(),Name :: atom(),Where :: list(), OrderBy :: list(), Limit :: integer(), Offset :: integer()) -> 
  Records :: list() | {'error', Reason :: string()}.
