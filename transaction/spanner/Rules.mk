d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), spanner-proto.proto)

OBJS-common := $(o)spanner-proto.o $(LIB-message) $(LIB-configuration) $(LIB-pbmessage)

OBJS-spanner-client := $(o)client.o $(OBJS-client) $(OBJS-common)

OBJS-spanner-server := $(o)server.o $(OBJS-replica) $(LIB-latency) $(OBJS-common)
