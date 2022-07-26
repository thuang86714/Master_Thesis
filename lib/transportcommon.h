// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport-common.h:
 *   template support for implementing transports
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Jialin Li        <lijl@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#pragma once

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/transport.h"

#include <map>
#include <unordered_map>

namespace dsnet {

template <typename ADDR>
class TransportCommon : public Transport
{

public:
    TransportCommon()
    {
        replicaAddressesInitialized = false;
    }

    virtual
    ~TransportCommon()
    {
        for (auto &kv : canonicalConfigs) {
            delete kv.second;
        }
    }

    virtual void
    RegisterReplica(TransportReceiver *receiver,
                    const dsnet::Configuration &config,
                    int groupIdx, int replicaIdx) override
    {
        ASSERT(groupIdx < config.g);
        ASSERT(replicaIdx < config.n);
        RegisterConfiguration(receiver, config, groupIdx, replicaIdx);
        RegisterInternal(receiver, &config.replica(groupIdx, replicaIdx),
                groupIdx, replicaIdx);
    }

    virtual void
    RegisterAddress(TransportReceiver *receiver,
                    const dsnet::Configuration &config,
                    const dsnet::ReplicaAddress *addr) override
    {
        RegisterConfiguration(receiver, config, -1, -1);
        RegisterInternal(receiver, addr, -1, -1);
    }

    virtual void
    ListenOnMulticast(TransportReceiver *receiver,
                      const dsnet::Configuration &config) override
    {
        // Transport that requires multicast support needs to override
        // this function
        return;
    }

    virtual bool
    SendMessage(TransportReceiver *src, const TransportAddress &dst,
                const Message &m) override
    {
        const ADDR &dstAddr = dynamic_cast<const ADDR &>(dst);
        return SendMessageInternal(src, dstAddr, m);
    }

    virtual bool
    SendMessageToReplica(TransportReceiver *src,
                         int replicaIdx,
                         const Message &m) override
    {
        ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
        int groupIdx = this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];
        return SendMessageToReplica(src, groupIdx, replicaIdx, m);
    }

    virtual bool
    SendMessageToReplica(TransportReceiver *src,
                         int groupIdx,
                         int replicaIdx,
                         const Message &m) override
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = replicaAddresses[cfg][groupIdx].find(replicaIdx);
        ASSERT(kv != replicaAddresses[cfg][groupIdx].end());

        return SendMessageInternal(src, kv->second, m);
    }

    virtual bool
    SendMessageToAll(TransportReceiver *src,
                     const Message &m)
    {
        ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
        int groupIdx = this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];

        return SendMessageToGroup(src, groupIdx, m);
    }

    virtual bool
    SendMessageToAllGroups(TransportReceiver *src,
                           const Message &m)
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        const ADDR &srcAddr = dynamic_cast<const ADDR &>(src->GetAddress());
        for (auto & kv : replicaAddresses[cfg]) {
            for (auto & kv2 : kv.second) {
                if (srcAddr == kv2.second) {
                    continue;
                }
                if (!SendMessageInternal(src, kv2.second, m)) {
                    return false;
                }
            }
        }
        return true;
    }

    virtual bool
    SendMessageToGroup(TransportReceiver *src,
                       int groupIdx,
                       const Message &m) override
    {
        return SendMessageToGroups(src, std::vector<int>{groupIdx}, m);
    }

    virtual bool
    SendMessageToGroups(TransportReceiver *src,
                        const std::vector<int> &groups,
                        const Message &m) override
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        const ADDR &srcAddr = dynamic_cast<const ADDR &>(src->GetAddress());
        for (int groupIdx : groups) {
            for (auto & kv : replicaAddresses[cfg][groupIdx]) {
                if (srcAddr == kv.second) {
                    continue;
                }
                if (!SendMessageInternal(src, kv.second, m)) {
                    return false;
                }
            }
        }
        return true;
    }

    virtual bool SendMessageToFC(TransportReceiver *src, const Message &m) override
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = fcAddresses.find(cfg);
        if (kv == fcAddresses.end()) {
            Panic("Configuration has no failure coordinator address");
        }

        return SendMessageInternal(src, kv->second, m);
    }

    virtual bool SendMessageToMulticast(TransportReceiver *src,
                                        const Message &m) override
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = multicastAddresses.find(cfg);
        if (kv == multicastAddresses.end()) {
            Panic("Configuration has no multicast address");
        }

        return SendMessageInternal(src, kv->second, m);
    }

    virtual bool SendMessageToSequencer(TransportReceiver *src,
                                        int index,
                                        const Message &m) override
    {
        const dsnet::Configuration *cfg = configurations[src];
        ASSERT(cfg != NULL);

        if (!replicaAddressesInitialized) {
            LookupAddresses();
        }

        auto kv = sequencerAddresses.find(cfg);
        if (kv == sequencerAddresses.end()) {
            Panic("Configuration has no sequencer addresses");
        }

        if (index >= (int)kv->second.size()) {
            Panic("Sequencer index exceed number of sequencer configured");
        }

        return SendMessageInternal(src, kv->second.at(index), m);
    }

    virtual TransportAddress *
    LookupAddress(const dsnet::ReplicaAddress &addr) const override
    {
        return new ADDR(LookupAddressInternal(addr));
    }

protected:
    virtual void RegisterInternal(TransportReceiver *receiver,
                                  const dsnet::ReplicaAddress *addr,
                                  int groupIdx, int replicaIdx) = 0;
    virtual bool SendMessageInternal(TransportReceiver *src,
                                     const ADDR &dst,
                                     const Message &m) = 0;
    virtual ADDR LookupAddressInternal(const dsnet::ReplicaAddress &addr) const = 0;

    std::unordered_map<dsnet::Configuration,
        dsnet::Configuration *> canonicalConfigs;
    std::map<TransportReceiver *,
        dsnet::Configuration *> configurations;
    std::map<const dsnet::Configuration *,
        std::map<int, std::map<int, ADDR> > > replicaAddresses; // config->groupid->replicaid->ADDR
    std::map<const dsnet::Configuration *,
        std::map<int, std::map<int, TransportReceiver *> > > replicaReceivers;
    std::map<const dsnet::Configuration *,
        std::vector<ADDR>> sequencerAddresses;
    std::map<const dsnet::Configuration *, ADDR> multicastAddresses;
    std::map<const dsnet::Configuration *, ADDR> fcAddresses;
    std::map<TransportReceiver *, int> replicaGroups;
    bool replicaAddressesInitialized;

    /* configs is a map of groupIdx to Configuration */
    virtual dsnet::Configuration *
    RegisterConfiguration(TransportReceiver *receiver,
                          const dsnet::Configuration &config,
                          int groupIdx,
                          int replicaIdx) {
        ASSERT(receiver != NULL);

        // Have we seen this configuration before? If so, get a
        // pointer to the canonical copy; if not, create one. This
        // allows us to use that pointer as a key in various
        // structures.
        dsnet::Configuration *canonical
            = canonicalConfigs[config];
        if (canonical == NULL) {
            canonical = new dsnet::Configuration(config);
            canonicalConfigs[config] = canonical;
        }
        // Record configuration
        configurations[receiver] = canonical;

        // If this is a replica, record the receiver
        if (replicaIdx != -1) {
            ASSERT(groupIdx != -1);
            replicaReceivers[canonical][groupIdx][replicaIdx] = receiver;
        }

        // Record which group this receiver belongs to
        replicaGroups[receiver] = groupIdx;

        // Mark replicaAddreses as uninitalized so we'll look up
        // replica addresses again the next time we send a message.
        replicaAddressesInitialized = false;
        return canonical;
    }

    virtual void
    LookupAddresses()
    {
        // Clear any existing list of addresses
        replicaAddresses.clear();
        sequencerAddresses.clear();
        multicastAddresses.clear();
        fcAddresses.clear();

        // For every configuration, look up all addresses and cache
        // them.
        for (auto &kv : canonicalConfigs) {
            dsnet::Configuration *cfg = kv.second;

            for (int i = 0; i < cfg->g; i++) {
                for (int j = 0; j < cfg->n; j++) {
                    const ADDR addr = LookupAddressInternal(cfg->replica(i, j));
                    replicaAddresses[cfg][i].insert(std::make_pair(j, addr));
                }
            }

            // Add sequencer addresses
            for (int i = 0; i < cfg->NumSequencers(); i++) {
                sequencerAddresses[cfg].push_back(LookupAddressInternal(cfg->sequencer(i)));
            }

            // And check if there's a multicast address
            if (cfg->multicast()) {
                multicastAddresses.insert(std::make_pair(cfg,
                            LookupAddressInternal(*cfg->multicast())));
            }

            // Failure coordinator address
            if (cfg->fc()) {
                fcAddresses.insert(std::make_pair(cfg, LookupAddressInternal(*cfg->fc())));
            }
        }

        replicaAddressesInitialized = true;
    }
};

} // namespace dsnet
