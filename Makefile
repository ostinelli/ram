PROJECT_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))

all:
	@rebar3 compile

compile_test:
	@rebar3 as test compile

clean:
	@rebar3 clean
	@find $(PROJECT_DIR)/. -name "erl_crash\.dump" | xargs rm -f
	@find $(PROJECT_DIR)/. -name "*\.beam" | xargs rm -f
	@find $(PROJECT_DIR)/. -name "*\.so" | xargs rm -f

dialyzer:
	@rebar3 dialyzer

run: all
ifdef sname
	@# 'make run sname=ram2
	@erl -pa `rebar3 path` \
	-name $(sname)@127.0.0.1 \
	-eval 'ram:start().'
else
	@erl -pa `rebar3 path` \
	-name ram@127.0.0.1 \
	-eval 'ram:start().'
endif

console: all
ifdef sname
	@# 'make console sname=ram2
	@erl -pa `rebar3 path` \
	-name $(sname)@127.0.0.1
else
	@erl -pa `rebar3 path` \
	-name ram@127.0.0.1
endif

test: compile_test
ifdef suite
	@# 'make test suite=ram_SUITE'
	ct_run -dir $(PROJECT_DIR)/test -logdir $(PROJECT_DIR)/test/results \
	-suite $(suite) \
	-pa `rebar3 as test path`
else
	ct_run -dir $(PROJECT_DIR)/test -logdir $(PROJECT_DIR)/test/results \
	-pa `rebar3 as test path`
endif
