// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * unreplicated.cc:
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

#include "common/replica.h"
#include "common/pbmessage.h"
#include "replication/unreplicated/replica.h"

#include "lib/message.h"
#include "lib/transport.h"

namespace dsnet {
namespace unreplicated {

using namespace proto;

void
UnreplicatedReplica::HandleRequest(const TransportAddress &remote,
                                   const proto::RequestMessage &msg)
{
    ToClientMessage m;
    ReplyMessage *reply = m.mutable_reply();

    auto kv = clientTable.find(msg.req().clientid());
    if (kv != clientTable.end()) {
        ClientTableEntry &entry = kv->second;
        if (msg.req().clientreqid() < entry.lastReqId) {
            return;
        }
        if (msg.req().clientreqid() == entry.lastReqId) {
            if (!(transport->SendMessage(this, remote, PBMessage(entry.reply)))) {
                Warning("Failed to resend reply to client");
            }
            return;
        }
    }

    ++last_op_;
    viewstamp_t v;
    v.view = 0;
    v.opnum = last_op_;
    v.sessnum = 0;
    v.msgnum = 0;
    log.Append(new LogEntry(v, LOG_STATE_RECEIVED, msg.req()));

    Execute(0, msg.req(), *reply);

    // The protocol defines these as required, even if they're not
    // meaningful.
    reply->set_view(0);
    reply->set_opnum(0);
    *(reply->mutable_req()) = msg.req();

    if (!(transport->SendMessage(this, remote, PBMessage(m))))
        Warning("Failed to send reply message");

    UpdateClientTable(msg.req(), m);
}

void
UnreplicatedReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                           const UnloggedRequestMessage &msg)
{
    ToClientMessage m;
    UnloggedReplyMessage *reply = m.mutable_unlogged_reply();

    ExecuteUnlogged(msg.req(), *reply);

    if (!(transport->SendMessage(this, remote, PBMessage(m))))
        Warning("Failed to send reply message");
}

UnreplicatedReplica::UnreplicatedReplica(Configuration config,
                                         int myIdx,
                                         bool initialize,
                                         Transport *transport,
                                         AppReplica *app)
    : Replica(config, 0, myIdx, initialize, transport, app),
      log(false)
{
    if (!initialize) {
        Panic("Recovery does not make sense for unreplicated mode");
    }

    this->status = STATUS_NORMAL;
    this->last_op_ = 0;
}

void
UnreplicatedReplica::ReceiveMessage(const TransportAddress &remote,
                                    void *buf, size_t size)
{
    static ToReplicaMessage replica_msg;
    static PBMessage m(replica_msg);

    m.Parse(buf, size);

    switch (replica_msg.msg_case()) {
        case ToReplicaMessage::MsgCase::kRequest:
            HandleRequest(remote, replica_msg.request());
            break;
        case ToReplicaMessage::MsgCase::kUnloggedRequest:
            HandleUnloggedRequest(remote, replica_msg.unlogged_request());
            break;
        default:
            Panic("Received unexpected message type: %u", replica_msg.msg_case());
    }
}

void
UnreplicatedReplica::UpdateClientTable(const Request &req,
				       const ToClientMessage &reply)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        // Duplicate request
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.reply = reply;
}

} // namespace dsnet::unreplicated
} // namespace dsnet
