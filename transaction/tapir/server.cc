// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/server.h:
 *   Tapir protocol server implementation.
 *
 * Copyright 2017 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Irene Zhang Ports  <iyzhang@cs.washington.edu>
 *                Jialin Li <lijl@cs.washington.edu>
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

#include "common/pbmessage.h"
#include "transaction/tapir/server.h"

#define RDebug(fmt, ...) Debug("[%d, %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)

namespace dsnet {
namespace transaction {
namespace tapir {

using namespace std;
using namespace proto;

TapirServer::TapirServer(const Configuration &config, int myShard, int myIdx,
                         bool initialize, Transport *transport, AppReplica *app) :
    Replica(config, myShard, myIdx, initialize, transport, app),
    view(0) { }

TapirServer::~TapirServer() { }

void
TapirServer::ReceiveMessage(const TransportAddress &remote,
                            void *buf, size_t size)
{
    static ToServerMessage server_msg;
    static PBMessage m(server_msg);

    m.Parse(buf, size);

    switch (server_msg.msg_case()) {
        case ToServerMessage::MsgCase::kProposeInconsistent:
            HandleProposeInconsistent(remote, server_msg.propose_inconsistent());
            break;
        case ToServerMessage::MsgCase::kFinalizeInconsistent:
            HandleFinalizeInconsistent(remote, server_msg.finalize_inconsistent());
            break;
        case ToServerMessage::MsgCase::kProposeConsensus:
            HandleProposeConsensus(remote, server_msg.propose_consensus());
            break;
        case ToServerMessage::MsgCase::kFinalizeConsensus:
            HandleFinalizeConsensus(remote, server_msg.finalize_consensus());
            break;
        default:
            Panic("Received unexpected message type %u",
                    server_msg.msg_case());
    }
}

void
TapirServer::HandleProposeInconsistent(const TransportAddress &remote,
                                       const ProposeInconsistentMessage &msg)
{
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreqid = msg.req().clientreqid();

    Debug("%lu:%lu Received inconsistent op: %s", clientid, clientreqid, (char *)msg.req().op().c_str());

    opid_t opid = make_pair(clientid, clientreqid);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(opid);
    ToClientMessage m;
    ReplyInconsistentMessage *reply = m.mutable_inconsistent_reply();
    if (entry != NULL) {
        // If we already have this op in our record, then just return it
        reply->set_view(entry->view);
        reply->set_replicaidx(this->replicaIdx);
        reply->mutable_opid()->set_clientid(clientid);
        reply->mutable_opid()->set_clientreqid(clientreqid);
    } else {
        // Otherwise, put it in our record as tentative
        record.Add(view, opid, msg.req(), RECORD_STATE_TENTATIVE);

        // 3. Return Reply
        reply->set_view(view);
        reply->set_replicaidx(this->replicaIdx);
        reply->mutable_opid()->set_clientid(clientid);
        reply->mutable_opid()->set_clientreqid(clientreqid);
    }

    // Send the reply
    transport->SendMessage(this, remote, PBMessage(m));

}

void
TapirServer::HandleFinalizeInconsistent(const TransportAddress &remote,
                                        const FinalizeInconsistentMessage &msg)
{
    uint64_t clientid = msg.opid().clientid();
    uint64_t clientreqid = msg.opid().clientreqid();

    Debug("%lu:%lu Received finalize inconsistent op", clientid, clientreqid);

    opid_t opid = make_pair(clientid, clientreqid);

    // Check record for the request
    RecordEntry *entry = record.Find(opid);
    if (entry != NULL && entry->state == RECORD_STATE_TENTATIVE) {
        // Mark entry as finalized
        record.SetStatus(opid, RECORD_STATE_FINALIZED);

        // Execute the operation
        Transaction t;
        t.ParseFromString(entry->request.op());
        ASSERT(t.op() == Transaction::COMMIT ||
               t.op() == Transaction::ABORT);
        txnarg_t arg;
        txnret_t ret;
        string result;
        arg.txnid = t.txnid();
        arg.type = t.op() == Transaction::COMMIT ? TXN_COMMIT : TXN_ABORT;
        app->ReplicaUpcall(0, t.txn(), result, &arg, &ret);
        //app->ExecInconsistentUpcall(entry->request.op());
        ASSERT(!ret.blocked);
        ASSERT(ret.unblocked_txns.empty());
        ASSERT(ret.commit);

        // Send the reply
        ToClientMessage m;
        ConfirmMessage *reply = m.mutable_confirm();
        reply->set_view(view);
        reply->set_replicaidx(this->replicaIdx);
        *reply->mutable_opid() = msg.opid();

        transport->SendMessage(this, remote, PBMessage(m));
    } else {
        // Ignore?
    }
}

void
TapirServer::HandleProposeConsensus(const TransportAddress &remote,
                                    const ProposeConsensusMessage &msg)
{
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreqid = msg.req().clientreqid();

    Debug("%lu:%lu Received consensus op: %s", clientid, clientreqid, (char *)msg.req().op().c_str());

    opid_t opid = make_pair(clientid, clientreqid);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(opid);
    ToClientMessage m;
    ReplyConsensusMessage *reply = m.mutable_consensus_reply();
    if (entry != NULL) {
        // If we already have this op in our record, then just return it
        reply->set_view(entry->view);
        reply->set_replicaidx(this->replicaIdx);
        reply->mutable_opid()->set_clientid(clientid);
        reply->mutable_opid()->set_clientreqid(clientreqid);
        reply->set_result(entry->result);
    } else {
        // Execute op
        Transaction t;
        t.ParseFromString(msg.req().op());
        ASSERT(t.op() == Transaction::PREPARE);
        txnarg_t arg;
        txnret_t ret;
        string result;
        arg.txnid = t.txnid();
        arg.type = TXN_PREPARE;
        app->ReplicaUpcall(0, t.txn(), result, &arg, &ret);
        //app->ExecConsensusUpcall(msg.req().op(), result);
        ReplyMessage replyMessage;
        replyMessage.set_status(ret.blocked ? ReplyMessage::RETRY :
                                (ret.commit ? ReplyMessage::OK : ReplyMessage::RETRY));
        replyMessage.set_reply(result);
        string s;
        replyMessage.SerializeToString(&s);


        // Put it in our record as tentative
        record.Add(view, opid, msg.req(), RECORD_STATE_TENTATIVE, s);


        // 3. Return Reply
        reply->set_view(view);
        reply->set_replicaidx(this->replicaIdx);
        reply->mutable_opid()->set_clientid(clientid);
        reply->mutable_opid()->set_clientreqid(clientreqid);
        reply->set_result(s);
    }

    // Send the reply
    transport->SendMessage(this, remote, PBMessage(m));
}

void
TapirServer::HandleFinalizeConsensus(const TransportAddress &remote,
                                     const FinalizeConsensusMessage &msg)
{
    uint64_t clientid = msg.opid().clientid();
    uint64_t clientreqid = msg.opid().clientreqid();

    Debug("%lu:%lu Received finalize consensus op", clientid, clientreqid);

    opid_t opid = make_pair(clientid, clientreqid);

    // Check record for the request
    RecordEntry *entry = record.Find(opid);
    if (entry != NULL) {
        // Mark entry as finalized
        record.SetStatus(opid, RECORD_STATE_FINALIZED);

        if (msg.result() != entry->result) {
            // Update the result
            entry->result = msg.result();
        }

        // Send the reply
        ToClientMessage m;
        ConfirmMessage *reply = m.mutable_confirm();
        reply->set_view(view);
        reply->set_replicaidx(this->replicaIdx);
        *reply->mutable_opid() = msg.opid();

        if (!transport->SendMessage(this, remote, PBMessage(m))) {
            Warning("Failed to send reply message");
        }
    } else {
        // Ignore?
        Warning("Finalize request for unknown consensus operation");
    }
}


} // namespace dsnet::transaction::tapir
} // namespace dsnet::store
} // namespace dsnet
