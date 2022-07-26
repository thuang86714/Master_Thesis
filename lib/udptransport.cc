// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.cc:
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"

#include <google/protobuf/message.h>
#include <event2/event.h>
#include <event2/thread.h>

#include <random>
#include <memory>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <net/if.h>
#include <linux/filter.h>
#include <linux/if_ether.h>
#include <sys/ioctl.h>
#include <netinet/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>

namespace dsnet {

static const size_t MAX_UDP_MESSAGE_SIZE = 9000; // XXX
static const int SOCKET_BUF_SIZE = 10485760;

typedef uint32_t Preamble;
static const Preamble NONFRAG_MAGIC = 0x20050318;
static const Preamble FRAG_MAGIC = 0x20101010;

using std::pair;

UDPTransportAddress::UDPTransportAddress(const std::string &s)
{
    memcpy(&addr, s.data(), sizeof(addr));
}

UDPTransportAddress::UDPTransportAddress(const sockaddr_in &addr)
    : addr(addr)
{
    memset((void *)addr.sin_zero, 0, sizeof(addr.sin_zero));
}

UDPTransportAddress *
UDPTransportAddress::clone() const
{
    UDPTransportAddress *c = new UDPTransportAddress(*this);
    return c;
}

bool operator==(const UDPTransportAddress &a, const UDPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const UDPTransportAddress &a, const UDPTransportAddress &b)
{
    return !(a == b);
}

bool operator<(const UDPTransportAddress &a, const UDPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}

UDPTransportAddress
UDPTransport::LookupAddressInternal(const dsnet::ReplicaAddress &addr) const
{
    int res;
    struct addrinfo hints;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_flags    = 0;
    struct addrinfo *ai;
    if ((res = getaddrinfo(addr.host.c_str(), addr.port.c_str(), &hints, &ai))) {
        Panic("Failed to resolve %s:%s: %s",
              addr.host.c_str(), addr.port.c_str(), gai_strerror(res));
    }
    if (ai->ai_addr->sa_family != AF_INET) {
        Panic("getaddrinfo returned a non IPv4 address");
    }
    UDPTransportAddress out =
        UDPTransportAddress(*((sockaddr_in *)ai->ai_addr));
    freeaddrinfo(ai);
    return out;
}

ReplicaAddress
UDPTransport::ReverseLookupAddress(const TransportAddress &addr) const
{
    const UDPTransportAddress *ua = dynamic_cast<const UDPTransportAddress *>(&addr);
    char buf[16];
    inet_ntop(AF_INET, &(ua->addr.sin_addr), buf, 16);
    return ReplicaAddress(std::string(buf), std::to_string(ntohs(ua->addr.sin_port)));
}

static void
BindToPort(int fd, const string &host, const string &port)
{
    struct sockaddr_in sin;

    if ((host == "") && (port == "any")) {
        // Set up the sockaddr so we're OK with any UDP socket
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = 0;
    } else {
        // Otherwise, look up its hostname and port number (which
        // might be a service name)
        struct addrinfo hints;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = AI_PASSIVE;
        struct addrinfo *ai;
        int res;
        if ((res = getaddrinfo(host.c_str(), port.c_str(),
                               &hints, &ai))) {
            Panic("Failed to resolve host/port %s:%s: %s",
                  host.c_str(), port.c_str(), gai_strerror(res));
        }
        ASSERT(ai->ai_family == AF_INET);
        ASSERT(ai->ai_socktype == SOCK_DGRAM);
        if (ai->ai_addr->sa_family != AF_INET) {
            Panic("getaddrinfo returned a non IPv4 address");
        }
        sin = *(sockaddr_in *)ai->ai_addr;

        freeaddrinfo(ai);
    }

    Notice("Binding to %s:%d", inet_ntoa(sin.sin_addr), htons(sin.sin_port));

    if (bind(fd, (sockaddr *)&sin, sizeof(sin)) < 0) {
        PPanic("Failed to bind to socket");
    }
}

UDPTransport::UDPTransport(double dropRate, double reorderRate,
                           event_base *evbase)
    : dropRate(dropRate), reorderRate(reorderRate)
{
    struct timeval tv;
    lastTimerId = 0;
    lastFragMsgId = 0;

    uniformDist = std::uniform_real_distribution<double>(0.0, 1.0);
    gettimeofday(&tv, NULL);
    randomEngine.seed(tv.tv_usec);
    reorderBuffer.valid = false;
    if (dropRate > 0) {
        Warning("Dropping packets with probability %g", dropRate);
    }
    if (reorderRate > 0) {
        Warning("Reordering packets with probability %g", reorderRate);
    }

    // Set up libevent
    event_set_log_callback(LogCallback);
    event_set_fatal_callback(FatalCallback);
    // XXX Hack for Naveen: allow the user to specify an existing
    // libevent base. This will probably not work exactly correctly
    // for error messages or signals, but that doesn't much matter...
    if (evbase) {
        libeventBase = evbase;
    } else {
        evthread_use_pthreads();
        libeventBase = event_base_new();
        evthread_make_base_notifiable(libeventBase);
    }

    // Set up signal handler
    signalEvents.push_back(evsignal_new(libeventBase, SIGTERM,
                                        SignalCallback, this));
    signalEvents.push_back(evsignal_new(libeventBase, SIGINT,
                                        SignalCallback, this));
    for (event *x : signalEvents) {
        event_add(x, NULL);
    }

}

UDPTransport::~UDPTransport()
{
    // XXX Shut down libevent?

    // for (auto kv : timers) {
    //     delete kv.second;
    // }
}

void
UDPTransport::RegisterInternal(TransportReceiver *receiver,
                               const dsnet::ReplicaAddress *addr,
                               int groupIdx, int replicaIdx)
{
    struct sockaddr_in sin;

    // Create socket
    int fd;
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        PPanic("Failed to create socket to listen");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK");
    }

    // Enable outgoing broadcast traffic
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_BROADCAST, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_BROADCAST on socket");
    }

    // Increase buffer size
    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_RCVBUF on socket");
    }
    if (setsockopt(fd, SOL_SOCKET,
                   SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_SNDBUF on socket");
    }

    if (addr != nullptr) {
        BindToPort(fd, addr->host, addr->port);
    } else {
        BindToPort(fd, "", "any");
    }

    // Set up a libevent callback
    event *ev = event_new(libeventBase, fd, EV_READ | EV_PERSIST,
                          SocketCallback, (void *)this);
    event_add(ev, NULL);
    listenerEvents.push_back(ev);

    // Tell the receiver its address
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    UDPTransportAddress *uaddr = new UDPTransportAddress(sin);
    receiver->SetAddress(uaddr);

    // Update mappings
    receivers[fd] = receiver;
    fds[receiver] = fd;

    Notice("Listening on UDP port %hu", ntohs(sin.sin_port));
}

void
UDPTransport::ListenOnMulticast(TransportReceiver *src,
                                const dsnet::Configuration &config)
{
    if (configurations.find(src) == configurations.end()) {
        Panic("Register address first before listening on multicast");
    }
    dsnet::Configuration *canonical = configurations.at(src);

    if (!canonical->multicast()) {
        // No multicast address specified
        return;
    }

    if (multicastFds.find(canonical) != multicastFds.end()) {
        // We're already listening
        return;
    }

    int fd;

    // Create socket
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        PPanic("Failed to create socket to listen for multicast");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK on multicast socket");
    }

    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_REUSEADDR on multicast socket");
    }

    // Increase buffer size
    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET,
                SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_RCVBUF on socket");
    }
    if (setsockopt(fd, SOL_SOCKET,
                SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_SNDBUF on socket");
    }
    // Bind to the specified address
    BindToPort(fd,
            canonical->multicast()->host,
            canonical->multicast()->port);

    // Set up a libevent callback
    event *ev = event_new(libeventBase, fd,
                          EV_READ | EV_PERSIST,
                          SocketCallback, (void *)this);
    event_add(ev, NULL);
    listenerEvents.push_back(ev);

    // Record the fd
    multicastFds[canonical] = fd;
    multicastConfigs[fd] = canonical;

    Notice("Listening for multicast requests on %s:%s",
           canonical->multicast()->host.c_str(),
           canonical->multicast()->port.c_str());
}

bool
UDPTransport::SendMessageInternal(TransportReceiver *src,
                                  const UDPTransportAddress &dst,
                                  const Message &m)
{
    sockaddr_in sin = dst.addr;

    // Serialize message
    size_t msg_len = sizeof(Preamble) + m.SerializedSize();
    char *buf = new char[msg_len];
    char *ptr = buf;
    *(Preamble *)ptr = NONFRAG_MAGIC;
    ptr += sizeof(Preamble);
    m.Serialize(ptr);

    int fd = fds[src];

    // XXX All of this assumes that the socket is going to be
    // available for writing, which since it's a UDP socket it ought
    // to be.
    if (msg_len <= MAX_UDP_MESSAGE_SIZE) {
        if (sendto(fd, buf, msg_len, 0,
                   (sockaddr *)&sin, sizeof(sin)) < 0) {
            PWarning("Failed to send message");
            goto fail;
        }
    } else {
        msg_len -= sizeof(Preamble);
        char *body_start = buf + sizeof(Preamble);
        int num_frags = ((msg_len - 1) / MAX_UDP_MESSAGE_SIZE) + 1;
        Notice("Sending large %s message in %d fragments",
               m.Type().c_str(), num_frags);
        uint64_t msg_id = ++lastFragMsgId;
        for (size_t frag_start = 0; frag_start < msg_len;
                frag_start += MAX_UDP_MESSAGE_SIZE) {
            size_t frag_len = std::min(msg_len - frag_start,
                                      MAX_UDP_MESSAGE_SIZE);
            size_t frag_header_len = 2 * sizeof(size_t) + sizeof(uint64_t) + sizeof(Preamble);
            char frag_buf[frag_len + frag_header_len];
            char *ptr = frag_buf;
            *((Preamble *)ptr) = FRAG_MAGIC;
            ptr += sizeof(Preamble);
            *((uint64_t *)ptr) = msg_id;
            ptr += sizeof(uint64_t);
            *((size_t *)ptr) = frag_start;
            ptr += sizeof(size_t);
            *((size_t *)ptr) = msg_len;
            ptr += sizeof(size_t);
            memcpy(ptr, &body_start[frag_start], frag_len);

            if (sendto(fd, frag_buf, frag_len + frag_header_len, 0,
                       (sockaddr *)&sin, sizeof(sin)) < 0) {
                PWarning("Failed to send message fragment %ld",
                         frag_start);
                goto fail;
            }
        }
    }

    delete [] buf;
    return true;

fail:
    delete [] buf;
    return false;
}

void
UDPTransport::Run()
{
    event_base_dispatch(libeventBase);
}

void
UDPTransport::Stop()
{
    event_base_loopbreak(libeventBase);
}

void
UDPTransport::OnReadable(int fd)
{
    const int BUFSIZE = 65536;
    ssize_t sz;
    char buf[BUFSIZE];
    sockaddr_in sender;
    socklen_t sender_size = sizeof(sender);

    sz = recvfrom(fd, buf, BUFSIZE, 0,
            (struct sockaddr *) &sender, &sender_size);
    if (sz == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        } else {
            PWarning("Failed to receive message from socket");
            return;
        }
    }

    ProcessPacket(fd, sender, sender_size, buf, sz);
}

void
UDPTransport::ProcessPacket(int fd, sockaddr_in sender, socklen_t sender_size,
                            char *buf, ssize_t sz)
{
    UDPTransportAddress sender_addr(sender);

    // Take a peek at the first field. If it's all zeros, this is
    // a fragment. Otherwise, we can process it directly
    ASSERT(sz > (long int)sizeof(Preamble));
    Preamble magic = *(Preamble *)buf;
    std::unique_ptr<char> copy_buf;
    void *msg_buf;
    size_t msg_size;

    if (magic == NONFRAG_MAGIC) {
        // Not a fragment
        msg_buf = buf + sizeof(Preamble);
        msg_size = sz - sizeof(Preamble);
    } else if (magic == FRAG_MAGIC) {
        // This is a fragment. Decode the header
        const char *ptr = buf;
        ptr += sizeof(Preamble);
        ASSERT(ptr - buf < sz);
        uint64_t msg_id = *((uint64_t *)ptr);
        ptr += sizeof(uint64_t);
        ASSERT(ptr - buf < sz);
        size_t frag_start = *((size_t *)ptr);
        ptr += sizeof(size_t);
        ASSERT(ptr - buf < sz);
        size_t msg_len = *((size_t *)ptr);
        ptr += sizeof(size_t);
        ASSERT(ptr - buf < sz);
        ASSERT(buf + sz - ptr == (ssize_t) std::min(msg_len - frag_start,
                                                    MAX_UDP_MESSAGE_SIZE));
        Notice("Received fragment of %zd byte packet %lx starting at %zd",
               msg_len, msg_id, frag_start);
        UDPTransportFragInfo &info = fragInfo[sender_addr];
        if (info.msgId == 0) {
            info.msgId = msg_id;
            info.data.clear();
        }
        if (info.msgId != msg_id) {
            ASSERT(msg_id > info.msgId);
            Warning("Failed to reconstruct packet %lx", info.msgId);
            info.msgId = msg_id;
            info.data.clear();
        }

        if (frag_start != info.data.size()) {
            Warning("Fragments out of order for packet %lx; "
                    "expected start %zd, got %zd",
                    msg_id, info.data.size(), frag_start);
            return;
        }

        info.data.append(string(ptr, buf + sz - ptr));
        if (info.data.size() == msg_len) {
            Debug("Completed packet reconstruction");
            msg_size = info.data.size();
            copy_buf = std::unique_ptr<char>(new char(msg_size));
            memcpy(copy_buf.get(), info.data.data(), msg_size);
            msg_buf = copy_buf.get();
            info.msgId = 0;
            info.data.clear();
        } else {
            return;
        }
    } else {
        Warning("Received packet with bad magic number");
        return;
    }

    // Dispatch
    if (dropRate > 0.0) {
        double roll = uniformDist(randomEngine);
        if (roll < dropRate) {
            Debug("Simulating packet drop of message");
            return;
        }
    }

    if (!reorderBuffer.valid && (reorderRate > 0.0)) {
        double roll = uniformDist(randomEngine);
        if (roll < reorderRate) {
            Debug("Simulating reorder of message");
            ASSERT(!reorderBuffer.valid);
            reorderBuffer.valid = true;
            reorderBuffer.addr = new UDPTransportAddress(sender_addr);
            reorderBuffer.message = std::string((const char *)msg_buf, msg_size);
            reorderBuffer.fd = fd;
            return;
        }
    }

deliver:
    // Was this received on a multicast fd?
    auto it = multicastConfigs.find(fd);
    if (it != multicastConfigs.end()) {
        // If so, deliver the message to all replicas for that
        // config, *except* if that replica was the sender of the
        // message.
        const dsnet::Configuration *cfg = it->second;
        for (auto &kv : replicaReceivers[cfg]) {
            shardnum_t groupIdx = kv.first;
            for (auto &kv2 : kv.second) {
                uint32_t replicaIdx = kv2.first;
                TransportReceiver *receiver = kv2.second;
                const UDPTransportAddress &raddr =
                    replicaAddresses[cfg][groupIdx].find(replicaIdx)->second;
                // Don't deliver a message to the sending replica
                if (raddr != sender_addr) {
                    receiver->ReceiveMessage(sender_addr, msg_buf, msg_size);
                }
            }
        }
    } else {
        TransportReceiver *receiver = receivers[fd];
        receiver->ReceiveMessage(sender_addr, msg_buf, msg_size);
    }

    if (reorderBuffer.valid) {
        reorderBuffer.valid = false;
        msg_size = reorderBuffer.message.size();
        copy_buf = std::unique_ptr<char>(new char(msg_size));
        memcpy(copy_buf.get(), reorderBuffer.message.data(), msg_size);
        msg_buf = copy_buf.get();
        fd = reorderBuffer.fd;
        sender_addr = *(reorderBuffer.addr);
        delete reorderBuffer.addr;
        Debug("Delivering reordered packet");
        goto deliver;       // XXX I am a bad person for this.
    }
}

int
UDPTransport::Timer(uint64_t ms, timer_callback_t cb)
{
    std::lock_guard<std::mutex> lck(this->timersLock);
    UDPTransportTimerInfo *info = new UDPTransportTimerInfo();

    struct timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;

    ++lastTimerId;

    info->transport = this;
    info->id = lastTimerId;
    info->cb = cb;
    info->ev = event_new(libeventBase, -1, 0,
                         TimerCallback, info);

    timers[info->id] = info;

    event_add(info->ev, &tv);

    return info->id;
}

bool
UDPTransport::CancelTimer(int id)
{
    std::lock_guard<std::mutex> l(this->timersLock);
    UDPTransportTimerInfo *info = timers[id];

    if (info == NULL) {
        return false;
    }

    event_del(info->ev);
    event_free(info->ev);
    timers.erase(info->id);
    delete info;

    return true;
}

void
UDPTransport::CancelAllTimers()
{
    while (!timers.empty()) {
        auto kv = timers.begin();
        CancelTimer(kv->first);
    }
}

void
UDPTransport::OnTimer(UDPTransportTimerInfo *info)
{
    {
        std::lock_guard<std::mutex> l(this->timersLock);
        timers.erase(info->id);
        event_del(info->ev);
        event_free(info->ev);
    }

    info->cb();

    delete info;
}

void
UDPTransport::SocketCallback(evutil_socket_t fd, short what, void *arg)
{
    UDPTransport *transport = (UDPTransport *)arg;
    if (what & EV_READ) {
        transport->OnReadable(fd);
    }
}

void
UDPTransport::TimerCallback(evutil_socket_t fd, short what, void *arg)
{
    UDPTransport::UDPTransportTimerInfo *info =
        (UDPTransport::UDPTransportTimerInfo *)arg;

    ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void
UDPTransport::LogCallback(int severity, const char *msg)
{
    Message_Type msgType;
    switch (severity) {
    case _EVENT_LOG_DEBUG:
        msgType = MSG_DEBUG;
        break;
    case _EVENT_LOG_MSG:
        msgType = MSG_NOTICE;
        break;
    case _EVENT_LOG_WARN:
        msgType = MSG_WARNING;
        break;
    case _EVENT_LOG_ERR:
        msgType = MSG_WARNING;
        break;
    default:
        NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void
UDPTransport::FatalCallback(int err)
{
    Panic("Fatal libevent error: %d", err);
}

void
UDPTransport::SignalCallback(evutil_socket_t fd, short what, void *arg)
{
    Notice("Terminating on SIGTERM/SIGINT");
    UDPTransport *transport = (UDPTransport *)arg;
    transport->Stop();
    exit(1);
}

} // namespace dsnet
