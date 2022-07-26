// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
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
#include "lib/transport.h"
#include "lib/transportcommon.h"

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <random>
#include <netinet/in.h>
#include <map>
#include <mutex>

namespace dsnet {

class UDPTransportAddress : public TransportAddress
{
public:
    UDPTransportAddress(const std::string &s);
    virtual UDPTransportAddress * clone() const override;

private:
    UDPTransportAddress(const sockaddr_in &addr);
    sockaddr_in addr;
    friend class UDPTransport;
    friend bool operator==(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator!=(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator<(const UDPTransportAddress &a,
                          const UDPTransportAddress &b);
};

class UDPTransport : public TransportCommon<UDPTransportAddress>
{
public:
    UDPTransport(double dropRate = 0.0, double reorderRate = 0.0,
                 event_base *evbase = nullptr);
    virtual ~UDPTransport();
    virtual void RegisterInternal(TransportReceiver *receiver,
                                  const dsnet::ReplicaAddress *addr,
                                  int groupIdx, int replicaIdx) override;
    virtual void ListenOnMulticast(TransportReceiver *receiver,
                                   const dsnet::Configuration &config) override;
    void Run() override;
    void Stop() override;
    int Timer(uint64_t ms, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;
    virtual ReplicaAddress
    ReverseLookupAddress(const TransportAddress &addr) const override;

private:
    struct UDPTransportTimerInfo
    {
        UDPTransport *transport;
        timer_callback_t cb;
        event *ev;
        int id;
    };

    double dropRate;
    double reorderRate;
    std::uniform_real_distribution<double> uniformDist;
    std::default_random_engine randomEngine;
    struct
    {
        bool valid;
        UDPTransportAddress *addr;
        std::string message;
        int fd;
    } reorderBuffer;
    event_base *libeventBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver*> receivers; // fd -> receiver
    std::map<TransportReceiver*, int> fds; // receiver -> fd
    std::map<const dsnet::Configuration *, int> multicastFds;
    std::map<int, const dsnet::Configuration *> multicastConfigs;
    int lastTimerId;
    std::map<int, UDPTransportTimerInfo *> timers;
    std::mutex timersLock;
    uint64_t lastFragMsgId;
    struct UDPTransportFragInfo
    {
        uint64_t msgId;
        string data;
    };
    std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;

    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;
    UDPTransportAddress
    LookupAddressInternal(const dsnet::ReplicaAddress &addr) const override;
    void OnReadable(int fd);
    void ProcessPacket(int fd, sockaddr_in sender, socklen_t senderSize,
                     char *buf, ssize_t sz);
    void OnTimer(UDPTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);
};

} // namespace dsnet
