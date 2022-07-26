d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	message.cc client.cc server.cc fcor.cc sequencer.cc)

PROTOS += $(addprefix $(d), eris-proto.proto)

OBJS-common := $(o)eris-proto.o $(o)message.o $(LIB-message) $(LIB-pbmessage)

OBJS-eris-client := $(o)client.o $(OBJS-client) $(OBJS-common)

OBJS-eris-server := $(o)server.o $(OBJS-replica) \
    $(LIB-configuration) $(LIB-latency) \
    $(OBJS-vr-client) $(OBJS-common)

OBJS-eris-fcor := $(o)fcor.o $(LIB-configuration) $(OBJS-replica) $(OBJS-common)

OBJS-eris-sequencer := $(o)sequencer.o
