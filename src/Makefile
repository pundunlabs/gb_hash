include ../vsn.mk
VSN=$(GB_HASH_VSN)

EBIN = ../ebin

RM ?= rm -f

APP_NAME = gb_hash
APP_FILE= $(APP_NAME).app
APP_SRC= $(APP_FILE).src
APP_TARGET= $(EBIN)/$(APP_FILE)

MODULES = \
	gb_hash \
	gb_hash_db \
	gb_hash_app \
	gb_hash_sup \
	gb_hash_register

ERL_FILES = $(MODULES:%=%.erl)

INCLUDE_DIRS = \
	../include

.PHONY: all clean

all: $(EBIN) $(APP_TARGET)
	erlc -I $(INCLUDE_DIRS) -o $(EBIN) $(ERL_FILES)

$(EBIN):
	mkdir ../ebin

$(APP_TARGET): $(APP_SRC) 
	@set -e; mods=`echo $(MODULES) | sed -e 's; ;,;g'` ; \
	sed -e "s;%MODULES%;$$mods;" $< | \
	sed -e 's;%VSN%;$(VSN);' > $@

clean:
	$(RM) ../ebin/*.app
	$(RM) ../ebin/*.beam
