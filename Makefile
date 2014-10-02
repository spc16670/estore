REBAR = ./rebar
DEPS = ./deps/*/ebin
RECORDS_PATH := $(CURDIR)/include

.PHONY: all get-deps test clean compile build-plt dialyze

all: deps compile

compile:
	@$(REBAR) compile

test:
	export ERL_FLAGS="-config estore"; $(REBAR) eunit skip_deps=true estore

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

