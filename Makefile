REBAR = ./rebar

.PHONY: all get-deps test clean compile build-plt dialyze

all: get-deps compile

compile:
	@$(REBAR) compile

test: compile
	@$(REBAR) eunit skip_deps=true

clean:
	@$(REBAR) clean

get-deps:
	@$(REBAR) get-deps

build-plt:
	@$(REBAR) build-plt

dialyze: compile
	@$(REBAR) dialyze
