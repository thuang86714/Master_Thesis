// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/granola/server.h:
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

#ifndef __GRANOLA_SERVER_H__
#define __GRANOLA_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "common/messageset.h"
#include "transaction/common/type.h"
#include "transaction/granola/granola-proto.pb.h"

#include <string>
#include <map>

namespace dsnet {
namespace transaction {
namespace granola {

typedef uint64_t timestamp_t;

struct TxnData {
    txnid_t txnid;
    bool indep;
    bool ro;
    timestamp_t proposed_ts;
    timestamp_t final_ts;
    bool ts_decided;
    proto::Status status;

    TxnData()
        : txnid(0), indep(false), ro(false),
        proposed_ts(0), final_ts(0), ts_decided(false) { }
    TxnData(txnid_t txnid,
            bool indep,
            bool ro,
            timestamp_t proposed_ts,
            timestamp_t final_ts,
            bool ts_decided)
        : txnid(txnid), indep(indep), ro(ro),
        proposed_ts(proposed_ts), final_ts(final_ts), ts_decided(ts_decided) { }
    TxnData(const TxnData &t)
        : txnid(t.txnid), indep(t.indep), ro(t.ro),
        proposed_ts(t.proposed_ts), final_ts(t.final_ts),
        ts_decided(t.ts_decided) { }
};

class GranolaLogEntry : public LogEntry
{
public:
    GranolaLogEntry(viewstamp_t viewstamp,
            LogEntryState state,
            const Request &request)
        : LogEntry(viewstamp, state, request) { }
    GranolaLogEntry(viewstamp_t viewstamp,
            LogEntryState state,
            const Request &request,
            const TxnData &txnData)
        : LogEntry(viewstamp, state, request),
        txnData(txnData) { }
    TxnData txnData;
};

class GranolaLog : public Log
{
public:
    GranolaLog();
    virtual LogEntry & Append(LogEntry *entry) override;
    LogEntry * Find(const std::pair<uint64_t, uint64_t> &reqid);

private:
    std::map<std::pair<uint64_t, uint64_t>, opnum_t> clientReqMap;
};

class GranolaServer : public Replica
{
public:
    GranolaServer(const Configuration &config, int myShard, int myIdx,
                  bool initialize, Transport *transport, AppReplica *app, bool locking=false);
    ~GranolaServer();

    void ReceiveMessage(const TransportAddress &remote,
                        void *buf, size_t size) override;

    void SetMode(bool locking) { this->locking = locking; }

public:
    GranolaLog log;

private:
    /* Replica states */
    view_t view;
    opnum_t lastOp;
    opnum_t lastCommitted;

    /* Locking mode */
    bool locking;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
        uint64_t lastReqId;
        bool replied;
        proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;


    /* Timestamps */
    timestamp_t localClock;

    /* On-going transactions (waiting for votes) */
    struct __ptxn_key_compare {
        bool operator() (const std::pair<opnum_t, timestamp_t> &lhs,
                         const std::pair<opnum_t, timestamp_t> &rhs) const
        {
            return lhs.second < rhs.second;
        }
    };
    // PendingTransactions sorted in ascending timestamp order
    std::set<std::pair<opnum_t, timestamp_t>, __ptxn_key_compare> pendingTransactions;

    /* Quorums */
    QuorumSet<viewstamp_t, proto::PrepareOKMessage> prepareOKQuorum;
    MessageSet<std::pair<uint64_t, uint64_t>, proto::VoteMessage> voteQuorum;

    /* Timeouts */
    Timeout *resendPrepareTimeout;
    const int RESEND_PREPARE_TIMEOUT = 10;
    std::map<opnum_t, Timeout *> voteTimeouts;
    const int VOTE_TIMEOUT = 10;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
			     const proto::RequestMessage &msg);
    void HandlePrepare(const TransportAddress &remote,
                       const proto::PrepareMessage &msg);
    void HandlePrepareOK(const TransportAddress &remote,
                         const proto::PrepareOKMessage &msg);
    void HandleCommit(const TransportAddress &remote,
                      const proto::CommitMessage &msg);
    void HandleVote(const TransportAddress &remote,
                    const proto::VoteMessage &msg);
    void HandleVoteRequest(const TransportAddress &remote,
                           const proto::VoteRequestMessage &msg);
    void HandleFinalTimestamp(const TransportAddress &remote,
                              const proto::FinalTimestampMessage &msg);

    void CommitUpTo(opnum_t opnum);
    void CheckVoteQuorum(GranolaLogEntry *entry);
    void ExecuteTxn(GranolaLogEntry *entry);
    void ExecuteTxns();
    void UpdateClientTable(const Request &req);
    void SendPrepare();
    void SendVoteRequest(LogEntry *entry);
    inline bool AmLeader();
};

} // namespace granola
} // namespace transaction
} // namespace dsnet

#endif /* _GRANOLA_SERVER_H_ */
