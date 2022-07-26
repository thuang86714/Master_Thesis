// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/granola/server.cc:
 *   Granola protocol server implementation.
 *
 * Copyright 2016 Jialin Li <lijl@cs.washington.edu>
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
#include "transaction/granola/server.h"

#define RDebug(fmt, ...) Debug("[%d, %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)

namespace dsnet {
namespace transaction {
namespace granola {

using namespace std;
using namespace proto;

GranolaLog::GranolaLog()
    : Log(false) { }

LogEntry &
GranolaLog::Append(LogEntry *entry)
{
    this->clientReqMap[std::make_pair(entry->request.clientid(),
                                      entry->request.clientreqid())] =
        entry->viewstamp.opnum;
    return Log::Append(entry);
}

LogEntry *
GranolaLog::Find(const std::pair<uint64_t, uint64_t> &reqid)
{
    if (clientReqMap.find(reqid) == clientReqMap.end()) {
        return nullptr;
    }

    return Log::Find(clientReqMap.at(reqid));
}

GranolaServer::GranolaServer(const Configuration&config, int myShard, int myIdx,
                             bool initialize, Transport *transport, AppReplica *app, bool locking)
    : Replica(config, myShard, myIdx, initialize, transport, app),
    locking(locking),
    prepareOKQuorum(config.QuorumSize()-1),
    voteQuorum(1)
{
    this->view = 0;
    this->lastOp = 0;
    this->lastCommitted = 0;
    this->localClock = 0;
    if (this->locking) {
        RNotice("Granola running in locking mode");
    }

    this->resendPrepareTimeout = new Timeout(transport, RESEND_PREPARE_TIMEOUT, [this, myShard, myIdx]() {
        RWarning("Prepare timeout! Resending Prepare");
        SendPrepare();
    });
}

GranolaServer::~GranolaServer()
{
    delete this->resendPrepareTimeout;
}

void
GranolaServer::ReceiveMessage(const TransportAddress &remote,
                              void *buf, size_t size)
{
    static ToServerMessage server_msg;
    static PBMessage m(server_msg);

    m.Parse(buf, size);

    switch (server_msg.msg_case()) {
        case ToServerMessage::MsgCase::kRequest:
            HandleClientRequest(remote, server_msg.request());
            break;
        case ToServerMessage::MsgCase::kPrepare:
            HandlePrepare(remote, server_msg.prepare());
            break;
        case ToServerMessage::MsgCase::kPrepareOk:
            HandlePrepareOK(remote, server_msg.prepare_ok());
            break;
        case ToServerMessage::MsgCase::kCommit:
            HandleCommit(remote, server_msg.commit());
            break;
        case ToServerMessage::MsgCase::kVote:
            HandleVote(remote, server_msg.vote());
            break;
        case ToServerMessage::MsgCase::kVoteRequest:
            HandleVoteRequest(remote, server_msg.vote_request());
            break;
        case ToServerMessage::MsgCase::kFinalTimestamp:
            HandleFinalTimestamp(remote, server_msg.final_timestamp());
            break;
        default:
            Panic("Received unexpected message type :%u",
              server_msg.msg_case());
    }
}

void
GranolaServer::HandleClientRequest(const TransportAddress &remote,
                                   const RequestMessage &msg)
{
    // Save client's address if not exist. Assume client
    // addresses never change.
    if (this->clientAddresses.find(msg.request().clientid()) == this->clientAddresses.end()) {
        this->clientAddresses.insert(std::pair<uint64_t, std::unique_ptr<TransportAddress> >(msg.request().clientid(), std::unique_ptr<TransportAddress>(remote.clone())));
    }

    // Non-leader replica ignore client requests
    if (!AmLeader()) {
        return;
    }

    // Check the client table to see if this is a duplicate request
    auto kv = this->clientTable.find(msg.request().clientid());
    if (kv != this->clientTable.end()) {
        ClientTableEntry &entry = kv->second;
        if (msg.request().clientreqid() < entry.lastReqId) {
            RDebug("Ignoring stale request");
            return;
        }
        if (msg.request().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply if we
            // have one. We might not have a reply to resend if we're
            // waiting for the other replicas; in that case, just
            // discard the request.
            if (entry.replied) {
                if (!(this->transport->SendMessage(this, remote,
                                                   PBMessage(entry.reply)))) {
                    RWarning("Failed to resend reply to client");
                }
                return;
            } else {
                RDebug("Received duplicate request but no reply available; ignoring");
                return;
            }
        }
    }

    // Update the client table
    UpdateClientTable(msg.request());

    ++this->lastOp;
    viewstamp_t v;
    v.view = this->view;
    v.opnum = this->lastOp;

    // Add the request to my log
    TxnData txnData;
    // XXX set proposed timestamp here. For now use the local clock.
    ++this->localClock;
    txnData.txnid = msg.txnid();
    txnData.indep = msg.indep();
    txnData.ro = msg.ro();
    txnData.proposed_ts = this->localClock;
    txnData.status = proto::COMMIT;
    if (msg.request().ops_size() == 1) {
        // Single-shard transactions use local timestamp
        // as the final timestamp.
        // XXX set the final timestamp here.
        txnData.final_ts = this->localClock;
        txnData.ts_decided = true;
    }

    if (!this->locking) {
        /* Insert transaction into pending transactions */
        this->pendingTransactions.insert(make_pair(v.opnum, txnData.proposed_ts));
    }

    this->log.Append(new GranolaLogEntry(v, LOG_STATE_PREPARED, msg.request(), txnData));

    // Set vote quorum size (only for multi-shard transactions)
    if (msg.request().ops_size() > 1) {
        this->voteQuorum.SetShardRequired(make_pair(msg.request().clientid(), msg.request().clientreqid()), msg.request().ops_size()-1);
    }

    // Send PrepareMessage to other replicas
    SendPrepare();
}

void
GranolaServer::HandlePrepare(const TransportAddress &remote,
                             const PrepareMessage &msg)
{
    ASSERT(!AmLeader());

    if (msg.opnum() <= this->lastOp) {
        // Resend the prepareOK message
        ToServerMessage m;
        PrepareOKMessage *prepareOKMessage = m.mutable_prepare_ok();
        prepareOKMessage->set_view(msg.view());
        prepareOKMessage->set_opnum(msg.opnum());
        prepareOKMessage->set_replica_num(this->replicaIdx);
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              PBMessage(m)))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
        return;
    }

    /*
    if (msg.opnum() > this->lastOp + 1) {
        Panic("State transfer not implemented yet");
    }

    ASSERT(msg.opnum() == this->lastOp + 1);
    */
    // XXX Hack here to get around state transfer
    while (this->lastOp + 1 < msg.opnum()) {
        this->lastOp++;
        this->log.Append(new GranolaLogEntry(viewstamp_t(msg.view(), this->lastOp),
                    LOG_STATE_EXECUTED,
                    Request()));
    }

    this->lastOp++;
    TxnData txnData;
    txnData.txnid = msg.txnid();
    txnData.indep = msg.indep();
    txnData.ro = msg.ro();
    txnData.proposed_ts = msg.timestamp();
    txnData.status = proto::COMMIT;
    if (msg.request().ops_size() == 1) {
        txnData.final_ts = txnData.proposed_ts;
        txnData.ts_decided = true;
    }

    if (!this->locking) {
        this->pendingTransactions.insert(make_pair(this->lastOp, txnData.proposed_ts));
    }
    this->log.Append(new GranolaLogEntry(viewstamp_t(msg.view(), this->lastOp),
                LOG_STATE_PREPARED,
                msg.request(),
                txnData));
    UpdateClientTable(msg.request());

    ToServerMessage m;
    PrepareOKMessage *prepareOKMessage = m.mutable_prepare_ok();
    prepareOKMessage->set_view(msg.view());
    prepareOKMessage->set_opnum(msg.opnum());
    prepareOKMessage->set_replica_num(this->replicaIdx);
    if (!this->transport->SendMessageToReplica(this,
                                               this->configuration.GetLeaderIndex(view),
                                               PBMessage(m))) {
        RWarning("Failed to send PrepareOK message to leader");
    }
}

void
GranolaServer::HandlePrepareOK(const TransportAddress &remote,
                               const PrepareOKMessage &msg)
{
    ASSERT(AmLeader());

    viewstamp_t vs = { msg.view(), msg.opnum() };

    if (this->prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replica_num(), msg)) {
        /* CommitUpTo will send Commit message */
        CommitUpTo(msg.opnum());
    }
}

void
GranolaServer::HandleCommit(const TransportAddress &remote,
                            const CommitMessage &msg)
{
    ASSERT(!AmLeader());

    if (msg.opnum() > this->lastOp) {
        // XXX Should do state transfer here
        //Panic("State transfer not implemented yet");
        return;
    }

    if (msg.opnum() <= this->lastCommitted) {
        // Already committed
        return;
    }

    CommitUpTo(msg.opnum());
}

void
GranolaServer::HandleVote(const TransportAddress &remote,
                          const VoteMessage &msg)
{
    ASSERT(msg.nshards() > 1);

    /* Only leader processes votes */
    if (!AmLeader()) {
        return;
    }

    pair<uint64_t, uint64_t> reqID = make_pair(msg.clientid(), msg.clientreqid());

    GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Find(reqID);
    if (entry) {
        if (entry->txnData.ts_decided) {
            // Final timestamp already decided
            return;
        }
    } else {
        // We haven't received this request yet. Set the
        // quorum size here.
        this->voteQuorum.SetShardRequired(reqID, msg.nshards()-1);
    }

    /* Replica num not relevant here, just use 0 */
    this->voteQuorum.Add(reqID, msg.shard_num(), 0, msg);

    if (entry) {
        CheckVoteQuorum(entry);
    }
}

void
GranolaServer::HandleVoteRequest(const TransportAddress &remote,
                                 const VoteRequestMessage &msg)
{
    /* Only leader process vote requests */
    if (!AmLeader()) {
        return;
    }

    pair<uint64_t, uint64_t> reqID = make_pair(msg.clientid(), msg.clientreqid());

    GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Find(reqID);
    if (entry == NULL) {
        return;
    }
    if (entry->state == LOG_STATE_COMMITTED || entry->state == LOG_STATE_EXECUTED) {
        ToServerMessage m;
        VoteMessage *voteMessage = m.mutable_vote();
        voteMessage->set_clientid(entry->request.clientid());
        voteMessage->set_clientreqid(entry->request.clientreqid());
        voteMessage->set_shard_num(this->groupIdx);
        voteMessage->set_nshards(entry->request.ops_size());
        voteMessage->set_status(entry->txnData.status);

        if (!this->transport->SendMessage(this, remote, PBMessage(m))) {
            RWarning("Failed to send VoteMessage to requester");
        }
    }
}

void
GranolaServer::HandleFinalTimestamp(const TransportAddress &remote,
                                    const FinalTimestampMessage &msg)
{
    ASSERT(!AmLeader());

    if (msg.opnum() > this->lastOp) {
        //Panic("State transfer not implemented yet");
        // XXX Hack here to work around state transfer
        while (this->lastOp < msg.opnum()) {
            this->lastOp++;
            this->log.Append(new GranolaLogEntry(viewstamp_t(msg.view(), this->lastOp),
                        LOG_STATE_EXECUTED,
                        Request()));
        }
        CommitUpTo(msg.opnum());
        if (!this->locking) {
            ExecuteTxns();
        }
    }

    GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Log::Find(msg.opnum());
    ASSERT(entry != NULL);
    entry->txnData.final_ts = msg.timestamp();
    entry->txnData.ts_decided = true;
    entry->txnData.status = msg.status();

    /* Requests with final timestamp are guaranteed to be committed */
    CommitUpTo(msg.opnum());
    if (this->locking) {
        ExecuteTxn(entry);
    } else {
        ExecuteTxns();
    }
}

void
GranolaServer::CommitUpTo(opnum_t opnum)
{
    if (this->lastCommitted < opnum && AmLeader()) {
        /* Leader send Commit message */
        ToServerMessage m;
        CommitMessage *commitMessage = m.mutable_commit();
        commitMessage->set_view(this->view);
        commitMessage->set_opnum(opnum);

        if (!this->transport->SendMessageToAll(this,
                                               PBMessage(m))) {
            RWarning("Failed to send COMMIT message to all replicas");
        }
    }

    if (opnum > this->lastCommitted) {
        this->resendPrepareTimeout->Stop();
    }

    while (this->lastCommitted < opnum) {
        this->lastCommitted++;
        GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Log::Find(this->lastCommitted);
        if (!entry) {
            RPanic("Did not find operation %lu in log", this->lastCommitted);
        }
        // XXX Hack here to handle state transfer
        if (entry->state == LOG_STATE_EXECUTED) {
            continue;
        }
        this->log.SetStatus(this->lastCommitted, LOG_STATE_COMMITTED);

        if (this->locking) {
            // In locking mode, call PREPARE into application
            ReplyMessage reply;
            txnarg_t arg;
            txnret_t ret;
            arg.txnid = entry->txnData.txnid;
            arg.type = TXN_PREPARE;

            Execute(entry->viewstamp.opnum, entry->request, reply, (void *)&arg, (void *)&ret);

            if (ret.blocked) {
                // Cannot acquire all locks
                entry->txnData.status = proto::CONFLICT;
            } else {
                entry->txnData.status = ret.commit ? COMMIT : ABORT;
            }

            reply.set_clientreqid(entry->request.clientreqid());
            reply.set_shard_num(this->groupIdx);
            reply.set_status(entry->txnData.status);
            /* Update client table */
            ClientTableEntry &cte = this->clientTable[entry->request.clientid()];
            if (cte.lastReqId <= entry->request.clientreqid()) {
                cte.lastReqId = entry->request.clientreqid();
                cte.reply = reply;
            }

            // XXX If ABORT or CONFLICT should immediately reply to client
        }

        /* Send vote if we are the leader and the transaction
         * is multi-shard.
         */
        if (AmLeader() && entry->request.ops_size() > 1) {
            ToServerMessage m;
            VoteMessage *voteMessage = m.mutable_vote();
            voteMessage->set_clientid(entry->request.clientid());
            voteMessage->set_clientreqid(entry->request.clientreqid());
            voteMessage->set_shard_num(this->groupIdx);
            voteMessage->set_nshards(entry->request.ops_size());
            voteMessage->set_status(entry->txnData.status);

            vector<int> shards;
            for (int i = 0; i < entry->request.ops_size(); i++) {
                /* Do not send to leader's own group */
                if ((int)entry->request.ops(i).shard() != this->groupIdx) {
                    shards.push_back(entry->request.ops(i).shard());
                }
            }

            if (!this->transport->SendMessageToGroups(this,
                                                      shards,
                                                      PBMessage(m))) {
                RWarning("Failed to send VoteMessage for request clientid %lu clientreqid %lu",
                         entry->request.clientid(), entry->request.clientreqid());
            }

            // Setup timeout
            ASSERT(this->voteTimeouts.find(this->lastCommitted) == this->voteTimeouts.end());
            Timeout *to = new Timeout(this->transport, VOTE_TIMEOUT, [this, entry]() {
                RWarning("Vote timeout");
                SendVoteRequest(entry);
            });
            this->voteTimeouts[this->lastCommitted] = to;
            to->Reset();
        }

        /* Check if we already have enough votes. */
        CheckVoteQuorum(entry);
    }
}

void
GranolaServer::CheckVoteQuorum(GranolaLogEntry *entry)
{
    ASSERT(entry != NULL);

    /* If already executed, ignore */
    if (entry->state == LOG_STATE_EXECUTED) {
        return;
    }

    if (!entry->txnData.ts_decided) {
        /* Check if we have all the votes to decide the
         * final timestamp (multi-shard only). Votes
         * should include this shard, so wait until
         * the request is replicated (COMMITTED state).
         * Also, only leader processes votes from other
         * shards.
         */
        ASSERT(entry->request.ops_size() > 1);
        auto msgs = this->voteQuorum.CheckForQuorum(make_pair(entry->request.clientid(),
                                                              entry->request.clientreqid()));
        if (this->configuration.GetLeaderIndex(entry->viewstamp.view) == this->replicaIdx &&
            entry->state == LOG_STATE_COMMITTED &&
            msgs != NULL) {
            // XXX Determine the final timestamp here
            entry->txnData.final_ts = entry->txnData.proposed_ts;
            entry->txnData.ts_decided = true;

            if (this->locking) {
                // Determine commit/abort/conflict decision of the transaction
                if (entry->txnData.status != proto::ABORT) {
                    for (const auto &kv : *msgs) {
                        ASSERT(kv.second.find(0) != kv.second.end());
                        if (kv.second.at(0).status() == proto::ABORT) {
                            entry->txnData.status = proto::ABORT;
                            break;
                        } else if (kv.second.at(0).status() == proto::CONFLICT) {
                            entry->txnData.status = proto::CONFLICT;
                        }
                    }
                }
            } else {
                /* Re-insert into pending transactions to put the
                 * transaction in the correct timestamp order. (only
                 * if final timestamp differs from proposed timestamp.
                 */
                if (entry->txnData.final_ts != entry->txnData.proposed_ts) {
                    auto iter = this->pendingTransactions.find(make_pair(entry->viewstamp.opnum,
                                                                         entry->txnData.proposed_ts));
                    ASSERT(iter != this->pendingTransactions.end());
                    this->pendingTransactions.erase(iter);
                    this->pendingTransactions.insert(make_pair(entry->viewstamp.opnum,
                                                               entry->txnData.final_ts));
                }
            }

            this->voteQuorum.Remove(make_pair(entry->request.clientid(),
                                              entry->request.clientreqid()));
            /* Send the final timestamp to other replicas. */
            ToServerMessage m;
            FinalTimestampMessage *finalTimestampMessage = m.mutable_final_timestamp();
            finalTimestampMessage->set_view(this->view);
            finalTimestampMessage->set_opnum(entry->viewstamp.opnum);
            finalTimestampMessage->set_timestamp(entry->txnData.final_ts);
            finalTimestampMessage->set_status(entry->txnData.status);

            if (!this->transport->SendMessageToAll(this,
                                                   PBMessage(m))) {
                RWarning("Failed to send FinalTimestampMessage");
            }
        }
    }

    /* Try executing transactions */
    if (entry->txnData.ts_decided) {
        if (this->voteTimeouts.find(entry->viewstamp.opnum) != this->voteTimeouts.end()) {
            // Cancel timeout now
            this->voteTimeouts[entry->viewstamp.opnum]->Stop();
            delete this->voteTimeouts[entry->viewstamp.opnum];
            this->voteTimeouts.erase(entry->viewstamp.opnum);
        }
        if (this->locking) {
            // Locking mode can immediately commit/abort transaction
            ExecuteTxn(entry);
        } else {
            // In normal mode, execute transactions in timestamp order
            ExecuteTxns();
        }
    }
}
void
GranolaServer::ExecuteTxn(GranolaLogEntry *entry)
{
    ASSERT(entry != nullptr);
    if (entry->state == LOG_STATE_EXECUTED) {
        return;
    }
    ASSERT(entry->state == LOG_STATE_COMMITTED);
    if (entry->txnData.status == proto::COMMIT || entry->txnData.status == proto::ABORT) {
        ReplyMessage reply;
        txnarg_t arg;
        txnret_t ret;
        arg.txnid = entry->txnData.txnid;
        arg.type = entry->txnData.status == proto::COMMIT ? TXN_COMMIT : TXN_ABORT;
        Execute(entry->viewstamp.opnum, entry->request, reply, (void *)&arg, (void *)&ret);
        ASSERT(ret.commit == true);
        ASSERT(ret.blocked == false);
    }
    entry->state = LOG_STATE_EXECUTED;
    // Update client table and reply to client
    ClientTableEntry &cte = this->clientTable[entry->request.clientid()];
    if (cte.lastReqId <= entry->request.clientreqid()) {
        cte.lastReqId = entry->request.clientreqid();
        cte.replied = true;
        // Use reply from the prepare phase
        cte.reply.set_status(entry->txnData.status);

        // Only leader send reply
        if (this->configuration.GetLeaderIndex(entry->viewstamp.view) == this->replicaIdx) {
            auto it = this->clientAddresses.find(entry->request.clientid());
            if (it != this->clientAddresses.end()) {
                if (!this->transport->SendMessage(this,
                            *it->second,
                            PBMessage(cte.reply))) {
                    RWarning("Failed to send ReplyMessage to client");
                }
            }
        }
    }
}

void
GranolaServer::ExecuteTxns()
{
    auto iter = this->pendingTransactions.begin();

    /* Execute in timestamp order */
    while (iter != this->pendingTransactions.end()) {
        const GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Log::Find(iter->first);
        if (!entry) {
            RPanic("Did not find operation %lu in log", iter->first);
        }

        // Can only execute if the final timestamp is decided and
        // it is already committed
        if (entry->txnData.ts_decided && entry->state == LOG_STATE_COMMITTED) {
            ReplyMessage reply;
            txnarg_t arg;
            txnret_t ret;
            arg.txnid = entry->txnData.txnid;
            // XXX change it to the actual type of txn
            arg.type = TXN_INDEP;

            Execute(entry->viewstamp.opnum, entry->request, reply,
                    (void *)&arg, (void *)&ret);
            this->log.SetStatus(entry->viewstamp.opnum, LOG_STATE_EXECUTED);

            reply.set_clientreqid(entry->request.clientreqid());
            reply.set_shard_num(this->groupIdx);
            reply.set_status(proto::COMMIT);

            /* Update client table */
            ClientTableEntry &cte = this->clientTable[entry->request.clientid()];
            // XXX lastReqId should never be smaller once state transfer
            // is implemented
            if (cte.lastReqId <= entry->request.clientreqid()) {
                cte.lastReqId = entry->request.clientreqid();
                cte.replied = true;
                cte.reply = reply;
            }

            /* Send reply to client (leader only) */
            if (this->configuration.GetLeaderIndex(entry->viewstamp.view) == this->replicaIdx) {
                auto iter2 = this->clientAddresses.find(entry->request.clientid());
                if (iter2 != this->clientAddresses.end()) {
                    if (!this->transport->SendMessage(this,
                                *iter2->second,
                                PBMessage(reply))) {
                        RWarning("Failed to send ReplyMessage to client");
                    }
                }
            }

            /* Remote transaction from pending transactions */
            iter = this->pendingTransactions.erase(iter);
        } else {
            /* Transaction with lower timestamp cannot be executed,
             * subsequent transactions have to wait.
             */
            return;
        }
    }
}

void
GranolaServer::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = this->clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
}

void
GranolaServer::SendPrepare()
{
    GranolaLogEntry *entry = (GranolaLogEntry *)this->log.Log::Find(this->lastOp);
    ASSERT(entry != nullptr);
    ToServerMessage m;
    PrepareMessage *prepareMessage = m.mutable_prepare();
    prepareMessage->set_view(entry->viewstamp.view);
    prepareMessage->set_opnum(entry->viewstamp.opnum);
    prepareMessage->set_txnid(entry->txnData.txnid);
    prepareMessage->set_indep(entry->txnData.indep);
    prepareMessage->set_ro(entry->txnData.ro);
    prepareMessage->set_timestamp(entry->txnData.proposed_ts);
    *(prepareMessage->mutable_request()) = entry->request;

    if (!this->transport->SendMessageToAll(this,
                                           PBMessage(m))) {
        RWarning("Failed to send Prepare message");
    }
    this->resendPrepareTimeout->Reset();
}

void
GranolaServer::SendVoteRequest(LogEntry *entry)
{
    auto votes = this->voteQuorum.GetMessages(make_pair(entry->request.clientid(), entry->request.clientreqid()));
    if (votes == NULL) {
        // All votes already received
        RWarning("All votes already received");
        return;
    }
    ToServerMessage m;
    VoteRequestMessage *voteRequestMessage = m.mutable_vote_request();
    voteRequestMessage->set_clientid(entry->request.clientid());
    voteRequestMessage->set_clientreqid(entry->request.clientreqid());

    vector<int> shards;
    for (auto it = entry->request.ops().begin();
         it != entry->request.ops().end();
         it++) {
        // Do not send to leader's own group,
        // and do not send to groups we already
        // received votes
        if ((int)it->shard() != this->groupIdx &&
            votes->find(it->shard()) == votes->end()) {
            shards.push_back(it->shard());
        }
    }
    if (shards.empty()) {
        RWarning("All votes already received");
        return;
    }

    if (!this->transport->SendMessageToGroups(this,
                                              shards,
                                              PBMessage(m))) {
        RWarning("Failed to send VoteMessage for request clientid %lu clientreqid %lu",
                 entry->request.clientid(), entry->request.clientreqid());
    }
}

inline bool
GranolaServer::AmLeader()
{
    return (this->configuration.GetLeaderIndex(this->view) == this->replicaIdx);
}

} // namespace granola
} // namespace transaction
} // namespace dsnet

