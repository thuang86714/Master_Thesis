d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), unreplicated-proto.proto)

OBJS-common := $(o)unreplicated-proto.o $(LIB-message) $(LIB-configuration) $(LIB-pbmessage)

OBJS-store-unreplicated-client := $(o)client.o $(OBJS-client) $(OBJS-common)

OBJS-store-unreplicated-server := $(o)server.o $(OBJS-replica) $(LIB-latency) $(OBJS-common)
