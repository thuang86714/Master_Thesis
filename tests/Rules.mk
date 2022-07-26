d := $(dir $(lastword $(MAKEFILE_LIST)))

include $(d)lib/Rules.mk $(d)replication/Rules.mk $(d)transaction/Rules.mk
