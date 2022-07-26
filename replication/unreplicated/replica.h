// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated/replica.h:
 *   dummy implementation of replication interface that just uses a
 *   single replica and passes commands directly to it
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

#ifndef _UNREPLICATED_REPLICA_H_
#define _UNREPLICATED_REPLICA_H_

#include "common/replica.h"
#include "replication/unreplicated/unreplicated-proto.pb.h"

#include "common/log.h"

namespace dsnet {
namespace unreplicated {

class UnreplicatedReplica : public Replica
{
public:
    UnreplicatedReplica(Configuration config, int myIdx,
                        bool initialize,
                        Transport *transport, AppReplica *app);
    void ReceiveMessage(const TransportAddress &remote,
                        void *buf, size_t size) override;

private:
    void HandleRequest(const TransportAddress &remote,
                       const proto::RequestMessage &msg);
    void HandleUnloggedRequest(const TransportAddress &remote,
                       const proto::UnloggedRequestMessage &msg);

    void UpdateClientTable(const Request &req,
			   const proto::ToClientMessage &reply);

    opnum_t last_op_;
    Log log;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        proto::ToClientMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;
};

} // namespace dsnet::unreplicated
} // namespace dsnet

#endif  /* _UNREPLICATED_REPLICA_H_ */
