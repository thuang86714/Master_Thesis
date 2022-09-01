d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc replica_OnHost.cc client.cc rdma_common.cc )

PROTOS += $(addprefix $(d), \
	    vr-proto.proto)

OBJS-vr-client := $(o)client.o $(o)vr-proto.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-configuration) $(LIB-pbmessage)

OBJS-vr-replica := $(o)replica.o $(o)vr-proto.o $(o)rdma_common.o \
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration) $(LIB-latency) $(LIB-pbmessage)

$(d)vr-replicaOnHost: $(o)replica_OnHost.o $(o)vr-proto.o $(o)rdma_common.o $(LIB-dpdktransport)
$(d)vr-replicaOnHost:	$(OBJS-replica) $(LIB-message)
$(d)vr-replicaOnHost: $(LIB-configuration) $(LIB-latency) $(LIB-pbmessage)

BINS += $(d)vr-replicaOnHost
