d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)pbft-test.cc

$(d)pbft-test: $(o)pbft-test.o \
	$(OBJS-pbft-replica) $(OBJS-pbft-client) \
        $(LIB-message) $(LIB-signature) $(LIB-simtransport) \
        $(GTEST_MAIN)

TEST_BINS += $(d)pbft-test
