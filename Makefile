include ./vsn.mk
VSN=$(GB_HASH_VSN)
APP_NAME = gb_hash

SUBDIRS = src test

.PHONY: all subdirs $(SUBDIRS) edoc eunit clean ct

all: subdirs

subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

edoc:
	erl -noshell -run edoc_run application "'$(APP_NAME)'" \
               '"."' '[{def,{vsn,"$(VSN)"}}, {source_path, ["src", "test"]}]'

eunit:
	erl -noshell -pa ebin \
	-eval 'eunit:test("ebin",[verbose])' \
	-s init stop

ct:
	mkdir -p test/ct/log/db
	ct_run -dir test/ct -logdir test/ct/log \
	-mnesia dir '"$(PWD)/test/ct/log/db"'

clean:
	rm -f ./ebin/*

realclean: clean

