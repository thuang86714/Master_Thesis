d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), kvClient.cc tpccClient.cc server.cc fcor.cc ycsb.cc benchclient.cc)

OBJS-all-app-clients := $(OBJS-kvstore-client) $(OBJS-tpcc-client)
OBJS-all-app-txnservers := $(OBJS-kvstore-txnserver) $(OBJS-tpcc-txnserver)
OBJS-all-proto-clients := $(OBJS-eris-client) $(OBJS-granola-client) $(OBJS-store-unreplicated-client) \
    $(OBJS-spanner-client) $(OBJS-tapir-client)
OBJS-all-proto-servers := $(OBJS-eris-server) $(OBJS-granola-server) $(OBJS-store-unreplicated-server) \
    $(OBJS-spanner-server) $(OBJS-tapir-server) $(OBJS-eris-fcor)

$(d)kvClient: $(OBJS-all-app-clients) $(OBJS-all-proto-clients) $(LIB-configuration) $(LIB-udptransport) \
    $(LIB-latency) $(o)kvClient.o $(o)benchclient.o

$(d)tpccClient: $(OBJS-all-app-clients) $(OBJS-all-proto-clients) $(LIB-configuration) $(LIB-udptransport) \
    $(LIB-latency) $(o)tpccClient.o

$(d)txnServer: $(OBJS-all-app-txnservers) $(OBJS-all-proto-servers) $(LIB-udptransport) $(o)server.o

$(d)fcor: $(OBJS-eris-fcor) $(OBJS-vr-replica) $(o)fcor.o

$(d)ycsb: $(OBJS-all-app-clients) $(OBJS-all-proto-clients) $(LIB-configuration) \
		$(LIB-udptransport) $(LIB-dpdktransport) $(LIB-latency) $(o)ycsb.o $(o)benchclient.o

BINS += $(d)kvClient $(d)tpccClient $(d)txnServer $(d)fcor $(d)ycsb
