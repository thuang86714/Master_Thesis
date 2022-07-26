// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * client.cc:
 *   interface to replication client stubs
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

#include "common/client.h"
#include "common/request.pb.h"
#include "lib/message.h"
#include "lib/transport.h"

#include <random>

namespace dsnet {

Client::Client(const Configuration &config, const ReplicaAddress &addr,
               Transport *transport, uint64_t clientid)
    : config(config), transport(transport)
{
    this->clientid = clientid;

    // Randomly generate a client ID
    // This is surely not the fastest way to get a random 64-bit int,
    // but it should be fine for this purpose.
    while (this->clientid == 0) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        this->clientid = dis(gen);
    }

    transport->RegisterAddress(this, config, &addr);
    node_addr_ = new ReplicaAddress(transport->ReverseLookupAddress(*transport_addr_));
}

Client::~Client()
{
    delete node_addr_;
}

void
Client::ReceiveMessage(const TransportAddress &remote,
                       void *buf, size_t size)
{
    Panic("Received unexpected message");
}

void
Client::Invoke(const std::map<shardnum_t, std::string> &requests,
               g_continuation_t continuation,
               void *arg)
{
    Panic("Protocol does not support multi-shard request");
}

void
Client::InvokeAsync(const string &request) {
    Panic("Protocol does not support InvokeAsync");
}

} // namespace dsnet
