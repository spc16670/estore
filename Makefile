REBAR = ./rebar

.PHONY: all get-deps test clean compile build-plt dialyze

all: deps compile

compile:
	@$(REBAR) compile

test: 
	@$(REBAR) eunit skip_deps=true

clean:
	@$(REBAR) clean

get-deps: $(REBAR)
	@$(REBAR) get-deps
	@$(REBAR) compile

deps: 
	@$(REBAR) get-deps
	@$(REBAR) compile

build-plt:
	@$(REBAR) build-plt

dialyze: compile
	@$(REBAR) dialyze

