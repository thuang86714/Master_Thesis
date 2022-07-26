d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), granola-proto.proto)

OBJS-common := $(o)granola-proto.o $(LIB-message) $(LIB-configuration) $(LIB-pbmessage)

OBJS-granola-client := $(o)client.o $(OBJS-client) $(OBJS-common)

OBJS-granola-server := $(o)server.o $(OBJS-replica) $(LIB-latency) $(OBJS-common)
