d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	sequencer.cc sequencer_main.cc)

OBJS-sequencer := $(o)sequencer.o $(LIB-message) $(LIB-configuration)

$(d)sequencer: $(o)sequencer_main.o $(OBJS-sequencer) $(LIB-udptransport) \
		$(OBJS-nopaxos-sequencer) $(OBJS-eris-sequencer) $(LIB-signature)

BINS += $(d)sequencer
