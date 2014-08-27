-module(estore_logging).

-export([
  log_term/2
]).


log_term(Level,Term) ->
  log_term(Level,Term,logging_enabled(Level)).

log_term(Level,Term,true) ->
  io:fwrite(atom_to_list(Level) ++ ": ~p~n",[Term]).

logging_enabled(_Level) ->
  true.
