// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos/client.h:
 *   Network Ordered Paxos client implementation
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 * 		  Jialin Li	   <lijl@cs.washington.edu>
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

#ifndef _NOPAXOS_CLIENT_H_
#define _NOPAXOS_CLIENT_H_

#include "common/client.h"
#include "common/quorumset.h"
#include "lib/configuration.h"
#include "replication/nopaxos/nopaxos-proto.pb.h"

namespace dsnet {
namespace nopaxos {

class NOPaxosClient : public Client
{
public:
    NOPaxosClient(const Configuration &config,
                  const ReplicaAddress &addr,
                  Transport *transport,
                  uint64_t clientid = 0);
    ~NOPaxosClient();
    void Invoke(const string &request,
                continuation_t continuation) override;
    void InvokeUnlogged(int replicaIdx,
                        const string &request,
                        continuation_t continuation,
                        timeout_continuation_t timeoutContinuation = nullptr,
                        uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
    void ReceiveMessage(const TransportAddress &remote,
                        void *buf, size_t size) override;

private:
    opnum_t lastReqID;

    struct PendingRequest
    {
        string request;
        opnum_t clientReqID;
        continuation_t continuation;
        timeout_continuation_t timeoutContinuation;
        inline PendingRequest(string request, opnum_t clientReqID,
                              continuation_t continuation)
            : request(request), clientReqID(clientReqID),
            continuation(continuation) { }
    };
    PendingRequest *pendingRequest;
    PendingRequest *pendingUnloggedRequest;
    Timeout *requestTimeout;
    Timeout *unloggedRequestTimeout;
    QuorumSet<int, proto::ReplyMessage> replyQuorum;

    void SendRequest();
    void ResendRequest();
    void CompleteOperation(const proto::ReplyMessage &msg);
    void HandleReply(const TransportAddress &remote,
                     const proto::ReplyMessage &msg);
    void HandleUnloggedReply(const TransportAddress &remote,
                             const proto::UnloggedReplyMessage &msg);
    void UnloggedRequestTimeoutCallback();
    bool IsLeader(view_t view, int replicaIdx);
};

} // namespace dsnet::nopaxos
} // namespace dsnet

#endif /* _NOPAXOS_CLIENT_H_ */
