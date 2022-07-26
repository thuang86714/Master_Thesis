d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)tombft-test.cc

$(d)tombft-test: $(o)tombft-test.o \
	$(OBJS-tombft-replica) $(OBJS-tombft-client) $(OBJS-sequencer) \
        $(LIB-message) $(LIB-signature) $(LIB-simtransport) \
        $(GTEST_MAIN)

TEST_BINS += $(d)tombft-test
