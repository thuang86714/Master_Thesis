d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	replica.cc client.cc message.cc sequencer.cc)

PROTOS += $(addprefix $(d), \
	  nopaxos-proto.proto)

OBJS-nopaxos-client := $(o)client.o $(o)nopaxos-proto.o $(o)message.o \
		       $(OBJS-client) $(LIB-message) \
		       $(LIB-configuration) $(LIB-pbmessage)

OBJS-nopaxos-replica := $(o)replica.o $(o)nopaxos-proto.o $(o)message.o \
		        $(OBJS-replica) $(LIB-message) \
		        $(LIB-configuration) $(LIB-pbmessage)

OBJS-nopaxos-sequencer := $(o)sequencer.o
