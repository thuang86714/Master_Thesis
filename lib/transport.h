// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport.h:
 *   message-passing network interface definition
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#include "lib/configuration.h"

#include <functional>
#include <list>
#include <map>
#include <unordered_map>

namespace dsnet {

class Message
{
public:
    virtual ~Message() { }
    virtual Message *Clone() const = 0;
    virtual std::string Type() const = 0;
    virtual size_t SerializedSize() const = 0;
    virtual void Parse(const void *buf, size_t size) = 0;
    // Serialize message and write to buf. Caller needs to make sure buf
    // is large enough to store the serialized message.
    virtual void Serialize(void *buf) const = 0;
};

class TransportAddress
{
public:
    virtual ~TransportAddress() { }
    virtual TransportAddress *clone() const = 0;
};

class TransportReceiver
{
public:
    virtual ~TransportReceiver();
    virtual void SetAddress(const TransportAddress *addr);
    virtual const TransportAddress& GetAddress();
    virtual void ReceiveMessage(const TransportAddress &remote,
                                void *buf, size_t size) = 0;

protected:
    const TransportAddress *transport_addr_;
};

typedef std::function<void (void)> timer_callback_t;

class Transport
{
public:
    virtual ~Transport() {}
    virtual void RegisterReplica(TransportReceiver *receiver,
                                 const Configuration &config,
                                 int groupIdx,
                                 int replicaIdx) = 0;
    /*
     * Set addr to nullptr if receiver can be bound to any address.
     */
    virtual void RegisterAddress(TransportReceiver *receiver,
                                 const Configuration &config,
                                 const ReplicaAddress *addr) = 0;
    virtual TransportAddress *
    LookupAddress(const dsnet::ReplicaAddress &addr) const = 0;
    virtual ReplicaAddress
    ReverseLookupAddress(const TransportAddress &addr) const = 0;
    virtual void ListenOnMulticast(TransportReceiver *receiver,
                                   const Configuration &config) = 0;
    virtual bool SendMessage(TransportReceiver *src,
                             const TransportAddress &dst,
                             const Message &m) = 0;
    /* Send message to a replica in the local/default(0) group */
    virtual bool SendMessageToReplica(TransportReceiver *src,
                                      int replicaIdx,
                                      const Message &m) = 0;
    /* Send message to a replica in a specific group */
    virtual bool SendMessageToReplica(TransportReceiver *src,
                                      int groupIdx,
                                      int replicaIdx,
                                      const Message &m) = 0;
    /* Send message to all replicas in the local/default(0) group */
    virtual bool SendMessageToAll(TransportReceiver *src,
                                  const Message &m) = 0;
    /* Send message to all replicas in all groups in the configuration */
    virtual bool SendMessageToAllGroups(TransportReceiver *src,
                                        const Message &m) = 0;
    /* Send message to all replicas in specific groups */
    virtual bool SendMessageToGroups(TransportReceiver *src,
                                     const std::vector<int> &groups,
                                     const Message &m) = 0;
    /* Send message to all replicas in a single group */
    virtual bool SendMessageToGroup(TransportReceiver *src,
                                    int groupIdx,
                                    const Message &m) = 0;
    /* Send message to failure coordinator */
    virtual bool SendMessageToFC(TransportReceiver *src,
                                 const Message &m) = 0;
    /* Send message to multicast address */
    virtual bool SendMessageToMulticast(TransportReceiver *src,
                                        const Message &m) = 0;
    /* Send message to sequencer */
    virtual bool SendMessageToSequencer(TransportReceiver *src,
                                        int index,
                                        const Message &m) = 0;
    virtual int Timer(uint64_t ms, timer_callback_t cb) = 0;
    virtual bool CancelTimer(int id) = 0;
    virtual void CancelAllTimers() = 0;
    virtual void Run() = 0;
    virtual void Stop() = 0;
};

class Timeout
{
public:
    Timeout(Transport *transport, uint64_t ms, timer_callback_t cb);
    virtual ~Timeout();
    virtual void SetTimeout(uint64_t ms);
    virtual uint64_t Start();
    virtual uint64_t Reset();
    virtual void Stop();
    virtual bool Active() const;

private:
    Transport *transport;
    uint64_t ms;
    timer_callback_t cb;
    int timerId;
};

} // namespace dsnet
