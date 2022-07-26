d := $(dir $(lastword $(MAKEFILE_LIST)))

#
# gtest-based tests
#
# sgdxbc: where is workertasks-test.cc?
GTEST_SRCS += $(addprefix $(d), \
			  configuration-test.cc \
			  simtransport-test.cc \
			  signature-test.cc \
			  quorumset-test.cc)

PROTOS += $(d)simtransport-testmessage.proto

$(d)configuration-test: $(o)configuration-test.o $(LIB-configuration) $(GTEST_MAIN)

TEST_BINS += $(d)configuration-test

$(d)simtransport-test: $(o)simtransport-test.o $(LIB-simtransport) $(LIB-pbmessage) $(o)simtransport-testmessage.o $(GTEST_MAIN)

TEST_BINS += $(d)simtransport-test

$(d)signature-test: $(o)signature-test.o $(LIB-signature) $(LIB-message) $(LIB-configuration) $(GTEST_MAIN)

TEST_BINS += $(d)signature-test

$(d)quorumset-test: $(o)quorumset-test.o $(LIB-message) $(GTEST_MAIN)

TEST_BINS += $(d)quorumset-test
