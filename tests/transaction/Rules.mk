d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)eris-test.cc $(d)eris-protocol-test.cc $(d)granola-test.cc \
			  $(d)unreplicated-test.cc  $(d)spanner-test.cc $(d)tapir-test.cc \
			  $(d)kvtxn-test.cc $(d)kvstore-test.cc $(d)versionstore-test.cc \
			  $(d)lockserver-test.cc

COMMON-OBJS := $(OBJS-kvstore-client) $(OBJS-kvstore-txnserver) $(LIB-simtransport) $(GTEST_MAIN)

$(d)eris-protocol-test: $(o)eris-protocol-test.o \
    $(OBJS-eris-client) $(OBJS-eris-server) $(OBJS-eris-fcor) \
    $(OBJS-vr-replica) $(OBJS-eris-sequencer) $(OBJS-sequencer) \
	$(LIB-simtransport) $(GTEST_MAIN)

$(d)eris-test: $(o)eris-test.o \
    $(COMMON-OBJS) \
    $(OBJS-eris-client) \
    $(OBJS-eris-server) \
	$(OBJS-eris-sequencer) $(OBJS-sequencer)

$(d)granola-test: $(o)granola-test.o \
    $(COMMON-OBJS) \
    $(OBJS-granola-client) \
    $(OBJS-granola-server)

$(d)unreplicated-test: $(o)unreplicated-test.o \
    $(COMMON-OBJS) \
    $(OBJS-store-unreplicated-client) \
    $(OBJS-store-unreplicated-server)

$(d)spanner-test: $(o)spanner-test.o \
    $(COMMON-OBJS) \
    $(OBJS-spanner-client) \
    $(OBJS-spanner-server)

$(d)tapir-test: $(o)tapir-test.o \
    $(COMMON-OBJS) \
    $(OBJS-tapir-client) \
    $(OBJS-tapir-server)

$(d)kvtxn-test: $(o)kvtxn-test.o \
	$(OBJS-kvstore-txnserver) \
	$(LIB-message) \
	$(LIB-store-common) \
	$(GTEST_MAIN)

$(d)kvstore-test: $(o)kvstore-test.o \
		$(LIB-transport) $(LIB-store-common) $(LIB-store-backend) \
		$(GTEST_MAIN)

$(d)versionstore-test: $(o)versionstore-test.o \
		$(LIB-transport) $(LIB-store-common) $(LIB-store-backend) \
		$(GTEST_MAIN)

$(d)lockserver-test: $(o)lockserver-test.o \
		$(LIB-transport) $(LIB-store-common) $(LIB-store-backend) \
		$(GTEST_MAIN)

TEST_BINS += $(d)eris-test $(d)eris-protocol-test $(d)granola-test $(d)unreplicated-test $(d)spanner-test $(d)tapir-test $(d)kvtxn-test $(d)kvstore-test $(d)versionstore-test $(d)lockserver-test
