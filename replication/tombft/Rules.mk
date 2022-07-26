d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc message.cc)

PROTOS += $(addprefix $(d), \
	    tombft-proto.proto)

OBJS-tombft-client := $(o)client.o $(o)tombft-proto.o $(o)message.o \
                   $(OBJS-client) $(LIB-message) \
                   $(LIB-configuration) $(LIB-pbmessage)

OBJS-tombft-replica := $(o)replica.o $(o)tombft-proto.o $(o)message.o \
                   $(OBJS-replica) $(LIB-message) \
                   $(LIB-configuration) $(LIB-pbmessage)
