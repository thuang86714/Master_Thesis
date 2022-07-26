// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * nopaxos/client.cc:
 *   Network Ordered Paxos Client
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *		  Jialin Li	   <lijl@cs.washington.edu>
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
#include "lib/message.h"
#include "replication/nopaxos/client.h"
#include "replication/nopaxos/message.h"

#include <netinet/ip.h>

namespace dsnet {
namespace nopaxos {

using namespace proto;

NOPaxosClient::NOPaxosClient(const Configuration &config,
                             const ReplicaAddress &addr,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, addr, transport, clientid),
    replyQuorum(config.QuorumSize())
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqID = 0;

    requestTimeout = new Timeout(transport, 100, [this]() {
        ResendRequest();
    });
    unloggedRequestTimeout = new Timeout(transport, 100, [this]() {
        UnloggedRequestTimeoutCallback();
    });

}

NOPaxosClient::~NOPaxosClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
}

void
NOPaxosClient::Invoke(const string &request,
                      continuation_t continuation)
{
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqID;
    pendingRequest = new PendingRequest(request, lastReqID, continuation);

    SendRequest();
}

void
NOPaxosClient::InvokeUnlogged(int replicaIdx,
                              const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqID;
    pendingUnloggedRequest = new PendingRequest(request, lastReqID, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    ToReplicaMessage m;
    UnloggedRequestMessage *reqMsg = m.mutable_unlogged_request();
    reqMsg->mutable_req()->set_op(request);
    reqMsg->mutable_req()->set_clientid(clientid);
    reqMsg->mutable_req()->set_clientreqid(lastReqID);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, NOPaxosMessage(m));
}

void
NOPaxosClient::SendRequest()
{
    ToReplicaMessage m;
    RequestMessage *reqMsg = m.mutable_request();
    reqMsg->mutable_req()->set_op(pendingRequest->request);
    reqMsg->mutable_req()->set_clientid(clientid);
    reqMsg->mutable_req()->set_clientreqid(pendingRequest->clientReqID);
    reqMsg->mutable_req()->set_clientaddr(node_addr_->Serialize());
    reqMsg->set_msgnum(0);
    reqMsg->set_sessnum(0);

    if (config.NumSequencers() > 0) {
        transport->SendMessageToSequencer(this, 0,
                NOPaxosMessage(m, true));
    } else {
        transport->SendMessageToMulticast(this,
                NOPaxosMessage(m, true));
    }

    requestTimeout->Reset();
}

void
NOPaxosClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}

void
NOPaxosClient::ReceiveMessage(const TransportAddress &remote,
                              void *buf, size_t size)
{
    static ToClientMessage client_msg;
    static NOPaxosMessage m(client_msg);

    m.Parse(buf, size);

    switch (client_msg.msg_case()) {
        case ToClientMessage::MsgCase::kReply:
            HandleReply(remote, client_msg.reply());
            break;
        case ToClientMessage::MsgCase::kUnloggedReply:
            HandleUnloggedReply(remote, client_msg.unlogged_reply());
            break;
        default:
            Panic("Received unexpected message type %u",
                    client_msg.msg_case());
    }
}

void
NOPaxosClient::CompleteOperation(const proto::ReplyMessage &msg)
{
    ASSERT(msg.has_reply());
    requestTimeout->Stop();

    PendingRequest *req = pendingRequest;
    pendingRequest = NULL;

    req->continuation(req->request, msg.reply());
    replyQuorum.Clear();

    delete req;
}

void
NOPaxosClient::HandleReply(const TransportAddress &remote,
                           const proto::ReplyMessage &msg)
{
    if (pendingRequest == NULL) {
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqID) {
        return;
    }

    if (auto msgs = replyQuorum.AddAndCheckForQuorum(msg.clientreqid(),
                                                     msg.replicaidx(),
                                                     msg)) {
        bool hasLeader = false;
        view_t latestView = 0;
        sessnum_t latestSessnum = 0;
        int leaderIdx;
        int matching = 0;

        /* Find the leader reply in the latest view */
        for (auto &kv : *msgs) {
            // If found higher view/session, previous
            // leader message was in an older view.
            if (kv.second.view() > latestView) {
                latestView = kv.second.view();
                hasLeader = false;
            }
            if (kv.second.sessnum() > latestSessnum) {
                latestSessnum = kv.second.sessnum();
            }

            if (IsLeader(kv.second.view(), kv.first) &&
                kv.second.view() == latestView &&
                kv.second.sessnum() == latestSessnum) {
                hasLeader = true;
                leaderIdx = kv.first;
            }
        }

        if (hasLeader) {
            /* Do we having matching replies? */
            const proto::ReplyMessage &leaderMessage = msgs->at(leaderIdx);

            for (auto &kv : *msgs) {
                if (kv.second.view() == leaderMessage.view() &&
                    kv.second.sessnum() == leaderMessage.sessnum() &&
                    kv.second.opnum() == leaderMessage.opnum()) {
                    ASSERT(kv.second.clientreqid() == leaderMessage.clientreqid());
                    matching++;
                }
            }

            if (matching >= config.QuorumSize()) {
                CompleteOperation(leaderMessage);
            }
        }
    }
}

void
NOPaxosClient::HandleUnloggedReply(const TransportAddress &remote,
                                   const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
NOPaxosClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

bool
NOPaxosClient::IsLeader(view_t view, int replicaIdx)
{
    return (config.GetLeaderIndex(view) == replicaIdx);
}

} // namespace dsnet::nopaxos
} // namespace dsnet
