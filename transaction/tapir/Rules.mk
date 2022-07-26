d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc irclient.cc record.cc server.cc)

PROTOS += $(addprefix $(d), tapir-proto.proto)

OBJS-common := $(o)tapir-proto.o $(LIB-message) $(LIB-configuration) $(LIB-pbmessage)

OBJS-tapir-client := $(o)irclient.o $(o)client.o $(OBJS-client) $(OBJS-common)

OBJS-tapir-server := $(o)record.o $(o)server.o $(OBJS-replica) $(OBJS-common)
