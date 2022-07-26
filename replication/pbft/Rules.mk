d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	    pbft-proto.proto)

OBJS-pbft-client := $(o)client.o $(o)pbft-proto.o \
               $(OBJS-client) $(LIB-message) \
               $(LIB-configuration) $(LIB-signature) $(LIB-pbmessage)

OBJS-pbft-replica := $(o)replica.o $(o)pbft-proto.o \
               $(OBJS-replica) $(LIB-message) \
               $(LIB-configuration) $(LIB-signature) $(LIB-pbmessage)
