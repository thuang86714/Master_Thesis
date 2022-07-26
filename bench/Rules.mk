d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc benchmark.cc replica.cc)

OBJS-benchmark := $(o)benchmark.o \
                  $(LIB-message) $(LIB-latency)

$(d)client: $(o)client.o $(OBJS-benchmark) $(LIB-udptransport) $(LIB-dpdktransport)
$(d)client:	$(OBJS-vr-client) $(OBJS-fastpaxos-client) $(OBJS-unreplicated-client) $(OBJS-nopaxos-client)
$(d)client: $(OBJS-spec-client) $(OBJS-pbft-client) $(OBJS-tombft-client)

$(d)replica: $(o)replica.o $(LIB-udptransport) $(LIB-dpdktransport)
$(d)replica: $(OBJS-vr-replica) $(OBJS-fastpaxos-replica) $(OBJS-unreplicated-replica) $(OBJS-nopaxos-replica)
$(d)replica: $(OBJS-spec-replica) $(OBJS-pbft-replica) $(OBJS-tombft-replica)

BINS += $(d)client $(d)replica
