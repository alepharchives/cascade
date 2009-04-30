
INCLUDE=include/
SRC=src/
EBIN=ebin/

ERL=erl
ERLC=erlc
EFLAGS=-DTEST

TEST_FLAGS=-pa $(EBIN) +Bd -noinput

SOURCES = cascade.erl

OBJECTS = cascade.beam

all: build

build: $(OBJECTS)

%.beam: $(SRC)%.erl
	@mkdir -p $(EBIN)
	$(ERLC) $(EFLAGS) -I $(INCLUDE) -o $(EBIN) $<

test: build
	@$(ERL) $(TEST_FLAGS) -eval 'eunit:test({dir, "$(EBIN)"}), init:stop().'

