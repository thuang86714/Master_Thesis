// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/spanner/server.h:
 *   Spanner protocol server implementation.
 *
 * Copyright 2017 Jialin Li <lijl@cs.washington.edu>
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

#ifndef __SPANNER_SERVER_H__
#define __SPANNER_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "common/messageset.h"
#include "transaction/common/type.h"
#include "transaction/spanner/spanner-proto.pb.h"

#include <string>
#include <map>

namespace dsnet {
namespace transaction {
namespace spanner {

struct TxnData {
    txnid_t txnid;
    proto::RequestType type;
    bool decided;

    TxnData()
        : txnid(0), type(proto::UNKNOWN), decided(false) { }
    TxnData(txnid_t txnid, proto::RequestType type)
        : txnid(txnid), type(type), decided(false) { }
    TxnData(const TxnData &t)
        : txnid(t.txnid), type(t.type), decided(t.decided) { }
};

class SpannerLogEntry : public LogEntry
{
public:
    SpannerLogEntry(viewstamp_t viewstamp,
            LogEntryState state,
            const Request &request)
        : LogEntry(viewstamp, state, request) { }
    SpannerLogEntry(viewstamp_t viewstamp,
            LogEntryState state,
            const Request &request,
            const TxnData &txnData)
        : LogEntry(viewstamp, state, request),
        txnData(txnData) { }
    TxnData txnData;
};

class SpannerServer : public Replica
{
public:
    SpannerServer(const Configuration &config, int myShard, int myIdx,
                  bool initialize, Transport *transport, AppReplica *app);
    ~SpannerServer();

    void ReceiveMessage(const TransportAddress &remote,
                        void *buf, size_t size) override;

public:
    Log log;

private:
    /* Replica states */
    view_t view;
    opnum_t lastOp;
    opnum_t lastCommitted;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
	uint64_t lastReqId;
        bool replied;
	proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;


    /* Quorums */
    QuorumSet<viewstamp_t, proto::PrepareOKMessage> prepareOKQuorum;

    /* Timeouts */
    Timeout *resendPrepareTimeout;
    const int RESEND_PREPARE_TIMEOUT = 10;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
			     const proto::RequestMessage &msg);
    void HandlePrepare(const TransportAddress &remote,
                       const proto::PrepareMessage &msg);
    void HandlePrepareOK(const TransportAddress &remote,
                         const proto::PrepareOKMessage &msg);
    void HandleCommit(const TransportAddress &remote,
                      const proto::CommitMessage &msg);

    void CommitUpTo(opnum_t opnum);
    void ExecuteTxn(SpannerLogEntry *entry);
    void UpdateClientTable(const Request &req);

    void SendPrepare();
    inline bool AmLeader();
};

} // namespace spanner
} // namespace transaction
} // namespace dsnet

#endif /* __SPANNER_SERVER_H__ */

