// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/client.cc:
 *   Viewstamped Replication clinet
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
#include "common/pbmessage.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"

namespace dsnet {
namespace vr {

VRClient::VRClient(const Configuration &config,
                   const ReplicaAddress &addr,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, addr, transport, clientid)
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;

    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest();
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

VRClient::~VRClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
VRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;
    pendingRequest = new PendingRequest(request, reqId, continuation);

    SendRequest();
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::ToReplicaMessage m;
    proto::UnloggedRequestMessage *reqMsg = m.mutable_unlogged_request();
    reqMsg->mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg->mutable_req()->set_clientid(clientid);
    reqMsg->mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, PBMessage(m));
}

void
VRClient::InvokeAsync(const string &request)
{
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    proto::ToReplicaMessage m;
    proto::RequestMessage *reqMsg = m.mutable_request();
    reqMsg->mutable_req()->set_op(request);
    reqMsg->mutable_req()->set_clientid(this->clientid);
    reqMsg->mutable_req()->set_clientreqid(this->lastReqId);

    transport->SendMessageToAll(this, PBMessage(m));
}

void
VRClient::SendRequest()
{
    proto::ToReplicaMessage m;
    proto::RequestMessage *reqMsg = m.mutable_request();
    reqMsg->mutable_req()->set_op(pendingRequest->request);
    reqMsg->mutable_req()->set_clientid(clientid);
    reqMsg->mutable_req()->set_clientreqid(pendingRequest->clientReqId);

    // XXX Try sending only to (what we think is) the leader first
    transport->SendMessageToAll(this, PBMessage(m));

    requestTimeout->Reset();
}

void
VRClient::ResendRequest()
{
    Warning("Client timeout; resending request");
    SendRequest();
}


void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         void *buf, size_t size)
{
    static proto::ToClientMessage client_msg;
    static PBMessage m(client_msg);

    m.Parse(buf, size);

    switch (client_msg.msg_case()) {
        case proto::ToClientMessage::MsgCase::kReply:
            HandleReply(remote, client_msg.reply());
            break;
        case proto::ToClientMessage::MsgCase::kUnloggedReply:
            HandleUnloggedReply(remote, client_msg.unlogged_reply());
            break;
        default:
            Panic("Received unexpected message type: %u", client_msg.msg_case());
    }
}

void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{
    if (pendingRequest == NULL) {
        Debug("Received reply when no request was pending");
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqId) {
        Debug("Received reply for a different request");
        return;
    }

    Debug("Client received reply");

    requestTimeout->Stop();

    PendingRequest *req = pendingRequest;
    pendingRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    Debug("Client received unloggedReply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace dsnet
