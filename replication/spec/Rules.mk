d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc)

PROTOS += $(addprefix $(d), \
	    spec-proto.proto)

OBJS-spec-client := $(o)client.o $(o)spec-proto.o \
                    $(OBJS-client) $(LIB-message) \
                    $(LIB-configuration) $(LIB-pbmessage)

OBJS-spec-replica := $(o)replica.o $(o)spec-proto.o \
                     $(OBJS-replica) $(LIB-message) \
                     $(LIB-configuration) $(LIB-latency) $(LIB-pbmessage)
