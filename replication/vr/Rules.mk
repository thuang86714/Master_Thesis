d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc replica_OnHost.cc client.cc rdma_common.c)

PROTOS += $(addprefix $(d), \
	    vr-proto.proto)

OBJS-vr-client := $(o)client.o $(o)vr-proto.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-configuration) $(LIB-pbmessage)

OBJS-vr-replica := $(o)replica.o $(o)vr-proto.o $(o)rdma_common.o\
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration) $(LIB-latency) $(LIB-pbmessage)

OBJS-vr-replica := $(o)replica.o $(o)vr-proto.o $(o)rdma_common.o\
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration) $(LIB-latency) $(LIB-pbmessage)
