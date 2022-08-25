//all the VR slow path logic+ non-leader replica fast path logic should be executed on HostMachine
//There are at least 2 ways tom communicate to the attachted BF; messeagin sending and RDMA. I choose RDMA as the first potential approach
/* for RDMA server side of code
 * This is a RDMA server side code. 
 *
 * Author: Animesh Trivedi 
 *         atrivedi@apache.org 
 *
 * TODO: Cleanup previously allocated resources in case of an error condition
 */
/*
from client to server                                              from server to client
'a' config+myIdx+initialize+transport+nullApp                      'a' ack
'b' remote+Unlogged_request                                        'b' CloseBatch--PBMessage(lastPrepare)
'c' remote+Prepare                                                 'c' HandleUnlogged--ToClientMessage m
'd' remote+Commit                                                  'd' HandlePrepare--ToClientMessage m
'e' remote+RequestStateTransfer                                    'e' HandleStateTransfer--lastOp changed
'f' remote+StateTransfer                                           'f' HandleStartViewChange--ToReplicaMessage m
'g' remote+StartViewChange                                         'g' HandleDoViewChange--ToReplicaMessage m
'h' remote+DoViewChange                                            'h' HandleStartView--lastOp changed
'i' remote+StartView                                               'i' HandleRecovery--ToReplicaMessage m 
'j' remote+Recovery                                                'j' HandleRecovery--lastOp changed
'k' remote+RecoveryResponse                                        'k' Latency_Start(&executeAndReplyLatency)
'l' Closebatch                                                     'l' Latency_End(&executeAndReplyLatency)
'm' RequestStateTransfer                                           'm' CommitUpto--transport
'n' clientAddress.insert                                           'n'
'o' UpdateClientTable()                                            'o'
'p' LeaderUpCall()
'q' ++this->lastOp;
'r' log.Append()
's' CommitUpto(msg.opnum())
't' send lastop, batchcomplete=false,
resendPrepareTimeout->Reset();closeBatchTimeout->Stop()
'u' 
'v' NullCOmmitTimeout->start()
'w' NullCOmmitTimeout->Reset()
'x' CloseBatchTimeout->Start()
'y' CloseBatchTimeout->Stop()
'z' resendPrepareTimeout->Reset()
'A' HandleRequest()--clientAddress, updateclienttable()
'B' HandleRequest()--clientAddress, updateclienttable(), 
lastOp, new log entry, nullCommitTimeout->Reset();
'C' HandleRequest()--clientAddress, updateclienttable(), 
lastOp, new log entry, closeBatchTimeout->Start(), 
nullCommitTimeout->Reset()
'D' HandlePrepareOk--RequestStateTransfer()
'E' HandlePrepareOk--CommitUpTo(), nullCommitTimeout->Reset();
*/
#include "common/replica.h"
#include "replication/vr/replica.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "common/pbmessage.h"

#include <algorithm>
#include <random>

#include "rdma_common.h"

#define RDebug(fmt, ...) Debug("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
namespace dsnet {
namespace vr {

using namespace proto;

//Host Machine should be RDMA server
/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
//write a main, so during experiment, I will also run one process on Node10

	
VRReplica::VRReplica(Configuration config, int myIdx,
                     bool initialize,
                     Transport *transport, int batchSize,
                     AppReplica *app)
    : Replica(config, 0, myIdx, initialize, transport, app),
      batchSize(batchSize),
      log(false),
      prepareOKQuorum(config.QuorumSize()-1),
      startViewChangeQuorum(config.QuorumSize()-1),
      doViewChangeQuorum(config.QuorumSize()-1),
      recoveryResponseQuorum(config.QuorumSize())
{
    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastOp = 0;
    this->lastCommitted = 0;
    this->lastRequestStateTransferView = 0;
    this->lastRequestStateTransferOpnum = 0;
    lastBatchEnd = 0;
    batchComplete = true;

    if (batchSize > 1) {
        Notice("Batching enabled; batch size %d", batchSize);
    }

    this->viewChangeTimeout = new Timeout(transport, 5000, [this,myIdx]() {
            RWarning("Have not heard from leader; starting view change");
            StartViewChange(view+1);
        });
    this->nullCommitTimeout = new Timeout(transport, 1000, [this]() {
            SendNullCommit();
        });
    this->stateTransferTimeout = new Timeout(transport, 1000, [this]() {
            this->lastRequestStateTransferView = 0;
            this->lastRequestStateTransferOpnum = 0;
        });
    this->stateTransferTimeout->Start();
    this->resendPrepareTimeout = new Timeout(transport, 500, [this]() {
            ResendPrepare();
        });
    this->closeBatchTimeout = new Timeout(transport, 300, [this]() {
            CloseBatch();
        });
    this->recoveryTimeout = new Timeout(transport, 5000, [this]() {
            SendRecoveryMessages();
        });

    _Latency_Init(&requestLatency, "request");
    _Latency_Init(&executeAndReplyLatency, "executeAndReply");

    if (initialize) {
        if (AmLeader()) {
            nullCommitTimeout->Start();
        } else {
            viewChangeTimeout->Start();
        }
    } else {
        this->status = STATUS_RECOVERING;
        this->recoveryNonce = GenerateNonce();
        SendRecoveryMessages();
        recoveryTimeout->Start();
    }
}

VRReplica::~VRReplica()
{
    //Latency_Dump(&requestLatency);
    //Latency_Dump(&executeAndReplyLatency);

    delete viewChangeTimeout;
    delete nullCommitTimeout;
    delete stateTransferTimeout;
    delete resendPrepareTimeout;
    delete closeBatchTimeout;
    delete recoveryTimeout;

    for (auto &kv : pendingPrepares) {
        delete kv.first;
    }
    disconnect_and_cleanup()
}

uint64_t
VRReplica::GenerateNonce() const
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);
}

bool
VRReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}

void
VRReplica::CommitUpTo(opnum_t upto)
{   int timeleft = upto - lastCommitted;
    while (timeleft > 0) {
        Latency_Start(&executeAndReplyLatency);
        memset(src, 'k', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, wc, 1);
        //lastCommitted++;

        /* Find operation in log */
        const LogEntry *entry = log.Find(lastCommitted);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
        }

        /* Execute it */
        RDebug("Executing request " FMT_OPNUM, lastCommitted);
        ToClientMessage m;
        ReplyMessage *reply = m.mutable_reply();
        UpcallArg arg;
        arg.isLeader = AmLeader();
        Execute(lastCommitted, entry->request, *reply, &arg);

        reply->set_view(entry->viewstamp.view);
        reply->set_opnum(entry->viewstamp.opnum);
        reply->set_clientreqid(entry->request.clientreqid());

        /* Mark it as committed */
        log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

        // Store reply in the client table
        ClientTableEntry &cte =
            clientTable[entry->request.clientid()];
        if (cte.lastReqId <= entry->request.clientreqid()) {
            cte.lastReqId = entry->request.clientreqid();
            cte.replied = true;
            cte.reply = m;
        } else {
            // We've subsequently prepared another operation from the
            // same client. So this request must have been completed
            // at the client, and there's no need to record the
            // result.
        }

        /* Send reply */
	int n = entry->request.clientid();
        auto iter = clientAddresses.find(entry->request.clientid());
        if (iter != clientAddresses.end()) {
            //transport->SendMessage(this, *iter->second, PBMessage(m));
	    memcpy(src+1, &n, sizeof(int));
	    memcpy(src+1+sizeof(int), &m, sizeof(m));
	    timeleft--;
	    Latency_End(&executeAndReplyLatency);
	    if (timeleft>0){
		memset(src,'l',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, wc, 1);
	    }else{
		memset(src,'m',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, wc, 1);
	    }
	}else{
	    timeleft--;
	    Latency_End(&executeAndReplyLatency);
	    if (timeleft>0){
		memset(src,'n',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, wc, 1);
	    }else{
		memset(src,'o',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, wc, 1);
	    }
	}
    }
}

void
VRReplica::SendPrepareOKs(opnum_t oldLastOp)
{
    /* Send PREPAREOKs for new uncommitted operations */
    for (opnum_t i = oldLastOp; i <= lastOp; i++) {
        /* It has to be new *and* uncommitted */
        if (i <= lastCommitted) {
            continue;
        }

        const LogEntry *entry = log.Find(i);
        if (!entry) {
            RPanic("Did not find operation " FMT_OPNUM " in log", i);
        }
        ASSERT(entry->state == LOG_STATE_PREPARED);
        UpdateClientTable(entry->request);

        ToReplicaMessage m;
        PrepareOKMessage *reply = m.mutable_prepare_ok();
        reply->set_view(view);
        reply->set_opnum(i);
        reply->set_replicaidx(this->replicaIdx);
       
        RDebug("Sending PREPAREOK " FMT_VIEWSTAMP " for new uncommitted operation",
               reply->view(), reply->opnum());
        memset(src, 'q', 1);
	memcpy(src+1, &m, sizeof(m));
	rdma_server_send();
	process_work_completion_events(io_completion_channel, wc, 1);
	/*   
        if (!(transport->SendMessageToReplica(this,configuration.GetLeaderIndex(view),PBMessage(m)))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
	*/
    }
}


void
VRReplica::RequestStateTransfer()
{
    ToReplicaMessage m;
    RequestStateTransferMessage *r = m.mutable_request_state_transfer();
    r->set_view(view);
    r->set_opnum(lastCommitted);

    if ((lastRequestStateTransferOpnum != 0) &&
        (lastRequestStateTransferView == view) &&
        (lastRequestStateTransferOpnum == lastCommitted)) {
        RDebug("Skipping state transfer request " FMT_VIEWSTAMP
               " because we already requested it", view, lastCommitted);
	memset(src, 'a', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, wc, 1);
        return;
    }

    RNotice("Requesting state transfer: " FMT_VIEWSTAMP, view, lastCommitted);

    this->lastRequestStateTransferView = view;
    this->lastRequestStateTransferOpnum = lastCommitted;
    memset(src, 's', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    /*
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send RequestStateTransfer message to all replicas");
    }
    */
}

void
VRReplica::EnterView(view_t newview)
{
    RNotice("Entering new view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_NORMAL;
    lastBatchEnd = lastOp;
    batchComplete = true;
    memcpy(src+1, &view, sizeof(view));
    memcpy(src+1+sizeof(view), &status, sizeof(status));
    memcpy(src+1+sizeof(view)+sizeof(status), &lastBatchEnd, sizeof(lastBatchEnd));
    recoveryTimeout->Stop();

    if (AmLeader()) {
        viewChangeTimeout->Stop();
        nullCommitTimeout->Start();
	memset(src, 't', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, wc, 1);
    } else {
        viewChangeTimeout->Start();
        nullCommitTimeout->Stop();
        resendPrepareTimeout->Stop();
        closeBatchTimeout->Stop();
	memset(src, 'u', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, wc, 1);
    }

    prepareOKQuorum.Clear();
    startViewChangeQuorum.Clear();
    doViewChangeQuorum.Clear();
    recoveryResponseQuorum.Clear();
}

void
VRReplica::StartViewChange(view_t newview)
{
    RNotice("Starting view change for view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_VIEW_CHANGE;

    viewChangeTimeout->Reset();
    nullCommitTimeout->Stop();
    resendPrepareTimeout->Stop();
    closeBatchTimeout->Stop();
    memset(src, 'v', 1);
    memcpy(src+1, &view, sizeof(view));
    memcpy(src+1+sizeof(m), &status, sizeof(status));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    ToReplicaMessage m;
    StartViewChangeMessage *svc = m.mutable_start_view_change();
    svc->set_view(newview);
    svc->set_replicaidx(this->replicaIdx);
    svc->set_lastcommitted(lastCommitted);
    memset(src, 'w', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    /*
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send StartViewChange message to all replicas");
    }
    */
}


void
VRReplica::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
    memset(src, 'x', 1);
    memcpy(src+1, &clientTable, sizeof(clientTable));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
}

void
VRReplica::CloseBatch()
{
    ASSERT(AmLeader());
    ASSERT(lastBatchEnd < lastOp);

    //opnum_t batchStart = lastBatchEnd+1;

    RDebug("Sending batched prepare from " FMT_OPNUM
           " to " FMT_OPNUM,
           batchStart, lastOp);
    // Send prepare messages 
    PrepareMessage *p = lastPrepare.mutable_prepare();
    p->set_view(view);
    p->set_opnum(lastOp);
    p->set_batchstart(batchStart);
    p->clear_request();

    for (opnum_t i = batchStart; i <= lastOp; i++) {
        Request *r = p->add_request();
        const LogEntry *entry = log.Find(i);
        ASSERT(entry != NULL);
        ASSERT(entry->viewstamp.view == view);
        ASSERT(entry->viewstamp.opnum == i);
        *r = entry->request;
    }
    memset(src, 'y', 1);
    memcpy(src+1, &lastPrepare, sizeof(lastPrepare));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    /*
    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        RWarning("Failed to send prepare message to all replicas");
    }
    lastBatchEnd = lastOp;
    batchComplete = false;

    resendPrepareTimeout->Reset();
    closeBatchTimeout->Stop();
    */
}
    
/*
    switch (replica_msg.msg_case()) {
        case ToReplicaMessage::MsgCase::kRequest:
            HandleRequest(remote, replica_msg.request());
            break;
        case ToReplicaMessage::MsgCase::kUnloggedRequest:
            HandleUnloggedRequest(remote, replica_msg.unlogged_request());
            break;
        case ToReplicaMessage::MsgCase::kPrepare:
            HandlePrepare(remote, replica_msg.prepare());
            break;
        case ToReplicaMessage::MsgCase::kPrepareOk:
            HandlePrepareOK(remote, replica_msg.prepare_ok());
            break;
        case ToReplicaMessage::MsgCase::kCommit:
            HandleCommit(remote, replica_msg.commit());
            break;
        case ToReplicaMessage::MsgCase::kRequestStateTransfer:
            HandleRequestStateTransfer(remote,
                    replica_msg.request_state_transfer());
            break;
        case ToReplicaMessage::MsgCase::kStateTransfer:
            HandleStateTransfer(remote, replica_msg.state_transfer());
            break;
        case ToReplicaMessage::MsgCase::kStartViewChange:
            HandleStartViewChange(remote, replica_msg.start_view_change());
            break;
        case ToReplicaMessage::MsgCase::kDoViewChange:
            HandleDoViewChange(remote, replica_msg.do_view_change());
            break;
        case ToReplicaMessage::MsgCase::kStartView:
            HandleStartView(remote, replica_msg.start_view());
            break;
        case ToReplicaMessage::MsgCase::kRecovery:
            HandleRecovery(remote, replica_msg.recovery());
            break;
        case ToReplicaMessage::MsgCase::kRecoveryResponse:
            HandleRecoveryResponse(remote, replica_msg.recovery_response());
            break;
        default:
            RPanic("Received unexpected message type %u",
                    replica_msg.msg_case());
    }
}
*/


void
VRReplica::HandleUnloggedRequest(const TransportAddress &remote,
                                 const UnloggedRequestMessage &msg)
{
    if (status != STATUS_NORMAL) {
        // Not clear if we should ignore this or just let the request
        // go ahead, but this seems reasonable.
        RNotice("Ignoring unlogged request due to abnormal status");
        return;
    }

    ToClientMessage m;
    UnloggedReplyMessage *reply = m.mutable_unlogged_reply();

    Debug("Received unlogged request %s", (char *)msg.req().op().c_str());

    ExecuteUnlogged(msg.req(), *reply);
    memset(src, 'c', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    /*
    if (!(transport->SendMessage(this, remote, PBMessage(m))))
        Warning("Failed to send reply message");
    */
}

void
VRReplica::HandlePrepare(const TransportAddress &remote,
                         const PrepareMessage &msg)
{
    RDebug("Received PREPARE <" FMT_VIEW "," FMT_OPNUM "-" FMT_OPNUM ">",
           msg.view(), msg.batchstart(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPARE due to abnormal status");
        return;
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, wc, 1);
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring PREPARE due to stale view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, wc, 1);
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected PREPARE: I'm the leader of this view");
    }

    ASSERT(msg.batchstart() <= msg.opnum());
    ASSERT_EQ(msg.opnum()-msg.batchstart()+1, (unsigned int)msg.request_size());

    viewChangeTimeout->Reset();

    if (msg.opnum() <= this->lastOp) {
        RDebug("Ignoring PREPARE; already prepared that operation");
        // Resend the prepareOK message
        ToReplicaMessage m;
        PrepareOKMessage *reply = m.mutable_prepare_ok();
        reply->set_view(msg.view());
        reply->set_opnum(msg.opnum());
        reply->set_replicaidx(this->replicaIdx);
	memset(src, 'd', 1);
	memcpy(src+1, &m, sizeof(m));
	rdma_server_send();
        process_work_completion_events(io_completion_channel, wc, 1);
	/*
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              PBMessage(m)))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
	*/
        return;
    }

    if (msg.batchstart() > this->lastOp+1) {
        RequestStateTransfer();
        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
        return;
    }

    /* Add operations to the log */
    opnum_t op = msg.batchstart()-1;
    for (auto &req : msg.request()) {
        op++;
        if (op <= lastOp) {
            continue;
        }
        this->lastOp++;
        log.Append(new LogEntry(viewstamp_t(msg.view(), op), LOG_STATE_PREPARED, req));
        UpdateClientTable(req);//whether it's the last time to call update clienttable()
    }
    ASSERT(op == msg.opnum());

    /* Build reply and send it to the leader */
    ToReplicaMessage m;
    PrepareOKMessage *reply = m.mutable_prepare_ok();
    reply->set_view(msg.view());
    reply->set_opnum(msg.opnum());
    reply->set_replicaidx(this->replicaIdx);
    memset(src, 'd', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 1);
    /*
    if (!(transport->SendMessageToReplica(this,
                                          configuration.GetLeaderIndex(view),
                                          PBMessage(m)))) {
        RWarning("Failed to send PrepareOK message to leader");
    }
    */
}

/*
void
VRReplica::HandlePrepareOK(const TransportAddress &remote,
                           const PrepareOKMessage &msg)
{

    RDebug("Received PREPAREOK <" FMT_VIEW ", "
           FMT_OPNUM  "> from replica %d",
           msg.view(), msg.opnum(), msg.replicaidx());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPAREOK due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring PREPAREOK due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (!AmLeader()) {
        RWarning("Ignoring PREPAREOK because I'm not the leader");
        return;
    }

    viewstamp_t vs = { msg.view(), msg.opnum() };
    if (auto msgs =
        (prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg))) {
        /*
         * We have a quorum of PrepareOK messages for this
         * opnumber. Execute it and all previous operations.
         *
         * (Note that we might have already executed it. That's fine,
         * we just won't do anything.)
         *
         * This also notifies the client of the result.
         
        CommitUpTo(msg.opnum());

        if (msgs->size() >= (unsigned int)configuration.QuorumSize()) {
            return;
        }

        /*
         * Send COMMIT message to the other replicas.
         *
         * This can be done asynchronously, so it really ought to be
         * piggybacked on the next PREPARE or something.
         
        ToReplicaMessage m;
        CommitMessage *c = m.mutable_commit();
        c->set_view(this->view);
        c->set_opnum(this->lastCommitted);

        if (!(transport->SendMessageToAll(this, PBMessage(m)))) {
            RWarning("Failed to send COMMIT message to all replicas");
        }

        nullCommitTimeout->Reset();

        // XXX Adaptive batching -- make this configurable
        if (lastBatchEnd == msg.opnum()) {
            batchComplete = true;
            if  (lastOp > lastBatchEnd) {
                CloseBatch();
            }
        }
    }
}
*/
    
void
VRReplica::HandleCommit(const TransportAddress &remote,
                        const CommitMessage &msg)
{
    RDebug("Received COMMIT " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring COMMIT due to abnormal status");
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring COMMIT due to stale view");
        return;
    }

    if (msg.view() > this->view) {
        RequestStateTransfer();
        return;
    }

    if (AmLeader()) {
        RPanic("Unexpected COMMIT: I'm the leader of this view");
    }

    viewChangeTimeout->Reset();

    if (msg.opnum() <= this->lastCommitted) {
        RDebug("Ignoring COMMIT; already committed that operation");
        return;
    }

    if (msg.opnum() > this->lastOp) {
        RequestStateTransfer();
        return;
    }

    CommitUpTo(msg.opnum());
}


void
VRReplica::HandleRequestStateTransfer(const TransportAddress &remote,
                                      const RequestStateTransferMessage &msg)
{
    RDebug("Received REQUESTSTATETRANSFER " FMT_VIEWSTAMP,
           msg.view(), msg.opnum());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring REQUESTSTATETRANSFER due to abnormal status");
        return;
    }

    if (msg.view() > view) {
        RequestStateTransfer();
        return;
    }

    RNotice("Sending state transfer from " FMT_VIEWSTAMP " to "
            FMT_VIEWSTAMP,
            msg.view(), msg.opnum(), view, lastCommitted);

    ToReplicaMessage m;
    StateTransferMessage *reply = m.mutable_state_transfer();
    reply->set_view(view);
    reply->set_opnum(lastCommitted);

    log.Dump(msg.opnum()+1, reply->mutable_entries());

    transport->SendMessage(this, remote, PBMessage(m));
}

void
VRReplica::HandleStateTransfer(const TransportAddress &remote,
                               const StateTransferMessage &msg)
{
    RDebug("Received STATETRANSFER " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (msg.view() < view) {
        RWarning("Ignoring state transfer for older view");
        return;
    }

    opnum_t oldLastOp = lastOp;

    /* Install the new log entries */
    for (auto newEntry : msg.entries()) {
        if (newEntry.opnum() <= lastCommitted) {
            // Already committed this operation; nothing to be done.
#if PARANOID
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view == newEntry.view());
//          ASSERT(entry->request == newEntry.request());
#endif
        } else if (newEntry.opnum() <= lastOp) {
            // We already have an entry with this opnum, but maybe
            // it's from an older view?
            const LogEntry *entry = log.Find(newEntry.opnum());
            ASSERT(entry->viewstamp.opnum == newEntry.opnum());
            ASSERT(entry->viewstamp.view <= newEntry.view());

            if (entry->viewstamp.view == newEntry.view()) {
                // We already have this operation in our log.
                ASSERT(entry->state == LOG_STATE_PREPARED);
#if PARANOID
//              ASSERT(entry->request == newEntry.request());
#endif
            } else {
                // Our operation was from an older view, so obviously
                // it didn't survive a view change. Throw out any
                // later log entries and replace with this one.
                ASSERT(entry->state != LOG_STATE_COMMITTED);
                log.RemoveAfter(newEntry.opnum());
                lastOp = newEntry.opnum();
                oldLastOp = lastOp;

                viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
                log.Append(new LogEntry(vs, LOG_STATE_PREPARED, newEntry.request()));
            }
        } else {
            // This is a new operation to us. Add it to the log.
            ASSERT(newEntry.opnum() == lastOp+1);

            lastOp++;
            viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
            log.Append(new LogEntry(vs, LOG_STATE_PREPARED, newEntry.request()));
        }
    }


    if (msg.view() > view) {
        EnterView(msg.view());
    }

    /* Execute committed operations */
    ASSERT(msg.opnum() <= lastOp);
    CommitUpTo(msg.opnum());
    SendPrepareOKs(oldLastOp);

    // Process pending prepares
    std::list<std::pair<TransportAddress *, PrepareMessage> >pending = pendingPrepares;
    pendingPrepares.clear();
    for (auto & msgpair : pending) {
        RDebug("Processing pending prepare message");
        HandlePrepare(*msgpair.first, msgpair.second);
        delete msgpair.first;
    }
}

void
VRReplica::HandleStartViewChange(const TransportAddress &remote,
                                 const StartViewChangeMessage &msg)
{
    RDebug("Received STARTVIEWCHANGE " FMT_VIEW " from replica %d",
           msg.view(), msg.replicaidx());

    if (msg.view() < view) {
        RDebug("Ignoring STARTVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring STARTVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        RWarning("Received StartViewChange for view " FMT_VIEW
                 "from replica %d", msg.view(), msg.replicaidx());
        StartViewChange(msg.view());
    }

    ASSERT(msg.view() == view);

    if (auto msgs =
        startViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                   msg.replicaidx(),
                                                   msg)) {
        int leader = configuration.GetLeaderIndex(view);
        // Don't try to send a DoViewChange message to ourselves
        if (leader != this->replicaIdx) {
            ToReplicaMessage m;
            DoViewChangeMessage *dvc = m.mutable_do_view_change();
            dvc->set_view(view);
            dvc->set_lastnormalview(log.LastViewstamp().view);
            dvc->set_lastop(lastOp);
            dvc->set_lastcommitted(lastCommitted);
            dvc->set_replicaidx(this->replicaIdx);

            // Figure out how much of the log to include
            opnum_t minCommitted = std::min_element(
                msgs->begin(), msgs->end(),
                [](decltype(*msgs->begin()) a,
                   decltype(*msgs->begin()) b) {
                    return a.second.lastcommitted() < b.second.lastcommitted();
                })->second.lastcommitted();
            minCommitted = std::min(minCommitted, lastCommitted);

            log.Dump(minCommitted,
                     dvc->mutable_entries());

            if (!(transport->SendMessageToReplica(this, leader, PBMessage(m)))) {
                RWarning("Failed to send DoViewChange message to leader of new view");
            }
        }
    }
}


void
VRReplica::HandleDoViewChange(const TransportAddress &remote,
                              const DoViewChangeMessage &msg)
{
    RDebug("Received DOVIEWCHANGE " FMT_VIEW " from replica %d, "
           "lastnormalview=" FMT_VIEW " op=" FMT_OPNUM " committed=" FMT_OPNUM,
           msg.view(), msg.replicaidx(),
           msg.lastnormalview(), msg.lastop(), msg.lastcommitted());

    if (msg.view() < view) {
        RDebug("Ignoring DOVIEWCHANGE for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring DOVIEWCHANGE for current view");
        return;
    }

    if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
        // It's superfluous to send the StartViewChange messages here,
        // but harmless...
        RWarning("Received DoViewChange for view " FMT_VIEW
                 "from replica %d", msg.view(), msg.replicaidx());
        StartViewChange(msg.view());
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) == this->replicaIdx);

    auto msgs = doViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                        msg.replicaidx(),
                                                        msg);
    if (msgs != NULL) {
        // Find the response with the most up to date log, i.e. the
        // one with the latest viewstamp
        view_t latestView = log.LastViewstamp().view;
        opnum_t latestOp = log.LastViewstamp().opnum;
        DoViewChangeMessage *latestMsg = NULL;

        for (auto kv : *msgs) {
            DoViewChangeMessage &x = kv.second;
            if ((x.lastnormalview() > latestView) ||
                (((x.lastnormalview() == latestView) &&
                  (x.lastop() > latestOp)))) {
                latestView = x.lastnormalview();
                latestOp = x.lastop();
                latestMsg = &x;
            }
        }

        // Install the new log. We might not need to do this, if our
        // log was the most current one.
        if (latestMsg != NULL) {
            RDebug("Selected log from replica %d with lastop=" FMT_OPNUM,
                   latestMsg->replicaidx(), latestMsg->lastop());
            if (latestMsg->entries_size() == 0) {
                // There weren't actually any entries in the
                // log. That should only happen in the corner case
                // that everyone already had the entire log, maybe
                // because it actually is empty.
                ASSERT(lastCommitted == msg.lastcommitted());
                ASSERT(msg.lastop() == msg.lastcommitted());
            } else {
                if (latestMsg->entries(0).opnum() > lastCommitted+1) {
                    RPanic("Received log that didn't include enough entries to install it");
                }

                log.RemoveAfter(latestMsg->lastop()+1);
                log.Install(latestMsg->entries().begin(),
                            latestMsg->entries().end());
            }
        } else {
            RDebug("My log is most current, lastnormalview=" FMT_VIEW " lastop=" FMT_OPNUM,
                   log.LastViewstamp().view, lastOp);
        }

        // How much of the log should we include when we send the
        // STARTVIEW message? Start from the lowest committed opnum of
        // any of the STARTVIEWCHANGE or DOVIEWCHANGE messages we got.
        //
        // We need to compute this before we enter the new view
        // because the saved messages will go away.
        auto svcs = startViewChangeQuorum.GetMessages(view);
        opnum_t minCommittedSVC = std::min_element(
            svcs.begin(), svcs.end(),
            [](decltype(*svcs.begin()) a,
               decltype(*svcs.begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommittedDVC = std::min_element(
            msgs->begin(), msgs->end(),
            [](decltype(*msgs->begin()) a,
               decltype(*msgs->begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
            })->second.lastcommitted();
        opnum_t minCommitted = std::min(minCommittedSVC, minCommittedDVC);
        minCommitted = std::min(minCommitted, lastCommitted);

        EnterView(msg.view());

        ASSERT(AmLeader());

        lastOp = latestOp;
        if (latestMsg != NULL) {
            CommitUpTo(latestMsg->lastcommitted());
        }

        // Send a STARTVIEW message with the new log
        ToReplicaMessage m;
        StartViewMessage *sv = m.mutable_start_view();
        sv->set_view(view);
        sv->set_lastop(lastOp);
        sv->set_lastcommitted(lastCommitted);

        log.Dump(minCommitted, sv->mutable_entries());

        if (!(transport->SendMessageToAll(this, PBMessage(m)))) {
            RWarning("Failed to send StartView message to all replicas");
        }
    }
}

void
VRReplica::HandleStartView(const TransportAddress &remote,
                           const StartViewMessage &msg)
{
    RDebug("Received STARTVIEW " FMT_VIEW
          " op=" FMT_OPNUM " committed=" FMT_OPNUM " entries=%d",
          msg.view(), msg.lastop(), msg.lastcommitted(), msg.entries_size());
    RDebug("Currently in view " FMT_VIEW " op " FMT_OPNUM " committed " FMT_OPNUM,
          view, lastOp, lastCommitted);

    if (msg.view() < view) {
        RWarning("Ignoring STARTVIEW for older view");
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RWarning("Ignoring STARTVIEW for current view");
        return;
    }

    ASSERT(configuration.GetLeaderIndex(msg.view()) != this->replicaIdx);

    if (msg.entries_size() == 0) {
        ASSERT(msg.lastcommitted() == lastCommitted);
        ASSERT(msg.lastop() == msg.lastcommitted());
    } else {
        if (msg.entries(0).opnum() > lastCommitted+1) {
            RPanic("Not enough entries in STARTVIEW message to install new log");
        }

        // Install the new log
        log.RemoveAfter(msg.lastop()+1);
        log.Install(msg.entries().begin(),
                    msg.entries().end());
    }


    EnterView(msg.view());
    opnum_t oldLastOp = lastOp;
    lastOp = msg.lastop();

    ASSERT(!AmLeader());

    CommitUpTo(msg.lastcommitted());
    SendPrepareOKs(oldLastOp);
}

void
VRReplica::HandleRecovery(const TransportAddress &remote,
                          const RecoveryMessage &msg)
{
    RDebug("Received RECOVERY from replica %d", msg.replicaidx());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring RECOVERY due to abnormal status");
        return;
    }

    ToReplicaMessage m;
    RecoveryResponseMessage *reply = m.mutable_recovery_response();
    reply->set_replicaidx(this->replicaIdx);
    reply->set_view(view);
    reply->set_nonce(msg.nonce());
    if (AmLeader()) {
        reply->set_lastcommitted(lastCommitted);
        reply->set_lastop(lastOp);
        log.Dump(0, reply->mutable_entries());
    }

    if (!(transport->SendMessage(this, remote, PBMessage(m)))) {
        RWarning("Failed to send recovery response");
    }
    return;
}

void
VRReplica::HandleRecoveryResponse(const TransportAddress &remote,
                                  const RecoveryResponseMessage &msg)
{
    RDebug("Received RECOVERYRESPONSE from replica %d",
           msg.replicaidx());

    if (status != STATUS_RECOVERING) {
        RDebug("Ignoring RECOVERYRESPONSE because we're not recovering");
        return;
    }

    if (msg.nonce() != recoveryNonce) {
        RNotice("Ignoring recovery response because nonce didn't match");
        return;
    }

    auto msgs = recoveryResponseQuorum.AddAndCheckForQuorum(msg.nonce(),
                                                            msg.replicaidx(),
                                                            msg);
    if (msgs != NULL) {
        view_t highestView = 0;
        for (const auto &kv : *msgs) {
            if (kv.second.view() > highestView) {
                highestView = kv.second.view();
            }
        }

        int leader = configuration.GetLeaderIndex(highestView);
        ASSERT(leader != this->replicaIdx);
        auto leaderResponse = msgs->find(leader);
        if ((leaderResponse == msgs->end()) ||
            (leaderResponse->second.view() != highestView)) {
            RDebug("Have quorum of RECOVERYRESPONSE messages, "
                   "but still need to wait for one from the leader");
            return;
        }

        Notice("Recovery completed");

        log.Install(leaderResponse->second.entries().begin(),
                    leaderResponse->second.entries().end());
        EnterView(leaderResponse->second.view());
        lastOp = leaderResponse->second.lastop();
        CommitUpTo(leaderResponse->second.lastcommitted());
    }
}

//Below are for RDMA server

static int 
VRReplica::setup_client_resources()
{
	int ret = -1;
	if(!cm_client_id){
		rdma_error("Client id is still NULL \n");
		return -EINVAL;
	}
	/* We have a valid connection identifier, lets start to allocate 
	 * resources. We need: 
	 * 1. Protection Domains (PD)
	 * 2. Memory Buffers 
	 * 3. Completion Queues (CQ)
	 * 4. Queue Pair (QP)
	 * Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd = ibv_alloc_pd(cm_client_id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
	if (!pd) {
		rdma_error("Failed to allocate a protection domain errno: %d\n",
				-errno);
		return -errno;
	}
	debug("A new protection domain is allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		rdma_error("Failed to create an I/O completion event channel, %d\n",
				-errno);
		return -errno;
	}
	debug("An I/O completion event channel is created at %p \n", 
			io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!cq) {
		rdma_error("Failed to create a completion queue (cq), errno: %d\n",
				-errno);
		return -errno;
	}
	debug("Completion queue (CQ) is created at %p with %d elements \n", 
			cq, cq->cqe);
	/* Ask for the event for all activities in the completion queue*/
	ret = ibv_req_notify_cq(cq /* on which CQ */, 
			0 /* 0 = all event type, no filter*/);
	if (ret) {
		rdma_error("Failed to request notifications on CQ errno: %d \n",
				-errno);
		return -errno;
	}
	/* Now the last step, set up the queue pair (send, recv) queues and their capacity.
	 * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
       bzero(&qp_init_attr, sizeof qp_init_attr);
       qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
       qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
       qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
       qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
       qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
       /* We use same completion queue, but one can use different queues */
       qp_init_attr.recv_cq = cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
       if (ret) {
	       rdma_error("Failed to create QP due to errno: %d\n", -errno);
	       return -errno;
       }
       /* Save the reference for handy typing but is not required */
       client_qp = cm_client_id->qp;
       debug("Client QP created at %p\n", client_qp);
       return ret;
}

/* Starts an RDMA server by allocating basic connection resources */
static int 
VRReplica::start_rdma_server(struct sockaddr_in *server_addr) 
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return -errno;
	}
	debug("RDMA CM event channel is created successfully at %p \n", 
			cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return -errno;
	}
	debug("A RDMA connection id for the server is created \n");
	/* Explicit binding of rdma cm id to the socket credentials */
	ret = rdma_bind_addr(cm_server_id, (struct sockaddr*) server_addr);
	if (ret) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return -errno;
	}
	debug("Server RDMA CM id is successfully binded \n");
	/* Now we start to listen on the passed IP and port. However unlike
	 * normal TCP listen, this is a non-blocking call. When a new client is 
	 * connected, a new connection management (CM) event is generated on the 
	 * RDMA CM event channel from where the listening id was created. Here we
	 * have only one channel, so it is easy. */
	ret = rdma_listen(cm_server_id, 8); /* backlog = 8 clients, same as TCP, see man listen*/
	if (ret) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ",
				-errno);
		return -errno;
	}
	printf("Server is listening successfully at: %s , port: %d \n",
			inet_ntoa(server_addr->sin_addr),
			ntohs(server_addr->sin_port));
	/* now, we expect a client to connect and generate a RDMA_CM_EVNET_CONNECT_REQUEST 
	 * We wait (block) on the connection management event channel for 
	 * the connect event. 
	 */
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_CONNECT_REQUEST,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, ret = %d \n" , ret);
		return ret;
	}
	/* Much like TCP connection, listening returns a new connection identifier 
	 * for newly connected client. In the case of RDMA, this is stored in id 
	 * field. For more details: man rdma_get_cm_event 
	 */
	cm_client_id = cm_event->id;
	/* now we acknowledge the event. Acknowledging the event free the resources 
	 * associated with the event structure. Hence any reference to the event 
	 * must be made before acknowledgment. Like, we have already saved the 
	 * client id from "id" field before acknowledging the event. 
	 */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
		return -errno;
	}
	debug("A new RDMA client connection id is stored at %p\n", cm_client_id);
	return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int 
VRReplica::accept_client_connection()
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	struct sockaddr_in remote_sockaddr; 
	int ret = -1;
	if(!cm_client_id || !client_qp) {
		rdma_error("Client resources are not properly setup\n");
		return -EINVAL;
	}
	/* we prepare the receive buffer in which we will receive the client metadata*/
        client_metadata_mr = rdma_buffer_register(pd /* which protection domain */, 
			&client_metadata_attr /* what memory */,
			sizeof(client_metadata_attr) /* what length */, 
		       (IBV_ACCESS_LOCAL_WRITE) /* access permissions */);
	if(!client_metadata_mr){
		rdma_error("Failed to register client attr buffer\n");
		//we assume ENOMEM
		return -ENOMEM;
	}
	/* We pre-post this receive buffer on the QP. SGE credentials is where we 
	 * receive the metadata from the client */
	client_recv_sge.addr = (uint64_t) client_metadata_mr->addr; // same as &client_buffer_attr
	client_recv_sge.length = client_metadata_mr->length;
	client_recv_sge.lkey = client_metadata_mr->lkey;
	/* Now we link this SGE to the work request (WR) */
	bzero(&client_recv_wr, sizeof(client_recv_wr));
	client_recv_wr.sg_list = &client_recv_sge;
	client_recv_wr.num_sge = 1; // only one SGE
	ret = ibv_post_recv(client_qp /* which QP */,
		      &client_recv_wr /* receive work request*/,
		      &bad_client_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	debug("Receive buffer pre-posting is successful \n");
	/* Now we accept the connection. Recall we have not accepted the connection 
	 * yet because we have to do lots of resource pre-allocation */
       memset(&conn_param, 0, sizeof(conn_param));
       /* this tell how many outstanding requests can we handle */
       conn_param.initiator_depth = 3; /* For this exercise, we put a small number here */
       /* This tell how many outstanding requests we expect other side to handle */
       conn_param.responder_resources = 3; /* For this exercise, we put a small number */
       ret = rdma_accept(cm_client_id, &conn_param);
       if (ret) {
	       rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	       return -errno;
       }
       /* We expect an RDMA_CM_EVNET_ESTABLISHED to indicate that the RDMA  
	* connection has been established and everything is fine on both, server 
	* as well as the client sides.
	*/
        debug("Going to wait for : RDMA_CM_EVENT_ESTABLISHED event \n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_ESTABLISHED,
		       &cm_event);
        if (ret) {
		rdma_error("Failed to get the cm event, errnp: %d \n", -errno);
		return -errno;
	}
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	/* Just FYI: How to extract connection information */
	memcpy(&remote_sockaddr /* where to save */, 
			rdma_get_peer_addr(cm_client_id) /* gives you remote sockaddr */, 
			sizeof(struct sockaddr_in) /* max size */);
	printf("A new connection is accepted from %s \n", 
			inet_ntoa(remote_sockaddr.sin_addr));
	return ret;
}

/* This function sends server side buffer metadata to the connected client */
static int 
VRReplica::send_server_metadata_to_client() 
{
	struct ibv_wc wc;
	int ret = -1;
	/* Now, we first wait for the client to start the communication by 
	 * sending the server its metadata info. The server does not use it 
	 * in our example. We will receive a work completion notification for 
	 * our pre-posted receive request.
	 */
	ret = process_work_completion_events(io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to receive , ret = %d \n", ret);
		return ret;
	}
	/* if all good, then we should have client's buffer information, lets see */
	printf("Client side buffer information is received...\n");
	show_rdma_buffer_attr(&client_metadata_attr);
	printf("The client has requested buffer length of : %u bytes \n", 
			client_metadata_attr.length);
	/* We need to setup requested memory buffer. This is where the client will 
	* do RDMA READs and WRITEs. */
       server_buffer_mr = rdma_buffer_alloc(pd /* which protection domain */, 
		       client_metadata_attr.length /* what size to allocate */, 
		       (IBV_ACCESS_LOCAL_WRITE|
		       IBV_ACCESS_REMOTE_READ|
		       IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
       if(!server_buffer_mr){
	       rdma_error("Server failed to create a buffer \n");
	       /* we assume that it is due to out of memory error */
	       return -ENOMEM;
       }
       /* This buffer is used to transmit information about the above 
	* buffer to the client. So this contains the metadata about the server 
	* buffer. Hence this is called metadata buffer. Since this is already 
	* on allocated, we just register it. 
        * We need to prepare a send I/O operation that will tell the 
	* client the address of the server buffer. 
	*/
       server_metadata_attr.address = (uint64_t) server_buffer_mr->addr;
       server_metadata_attr.length = (uint32_t) server_buffer_mr->length;
       server_metadata_attr.stag.local_stag = (uint32_t) server_buffer_mr->lkey;
       server_metadata_mr = rdma_buffer_register(pd /* which protection domain*/, 
		       &server_metadata_attr /* which memory to register */, 
		       sizeof(server_metadata_attr) /* what is the size of memory */,
		       IBV_ACCESS_LOCAL_WRITE /* what access permission */);
       if(!server_metadata_mr){
	       rdma_error("Server failed to create to hold server metadata \n");
	       /* we assume that this is due to out of memory error */
	       return -ENOMEM;
       }
       /* We need to transmit this buffer. So we create a send request. 
	* A send request consists of multiple SGE elements. In our case, we only
	* have one 
	*/
       server_send_sge.addr = (uint64_t) &server_metadata_attr;
       server_send_sge.length = sizeof(server_metadata_attr);
       server_send_sge.lkey = server_metadata_mr->lkey;
       /* now we link this sge to the send request */
       bzero(&server_send_wr, sizeof(server_send_wr));
       server_send_wr.sg_list = &server_send_sge;
       server_send_wr.num_sge = 1; // only 1 SGE element in the array 
       server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
       server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
       /* This is a fast data path operation. Posting an I/O request */
       ret = ibv_post_send(client_qp /* which QP */, 
		       &server_send_wr /* Send request that we prepared before */, 
		       &bad_server_send_wr /* In case of error, this will contain failed requests */);
       if (ret) {
	       rdma_error("Posting of server metdata failed, errno: %d \n",
			       -errno);
	       return -errno;
       }
       /* We check for completion notification */
       ret = process_work_completion_events(io_completion_channel, &wc, 1);
       if (ret != 1) {
	       rdma_error("Failed to send server metadata, ret = %d \n", ret);
	       return ret;
       }
       debug("Local buffer metadata has been sent to the client \n");
       return 0;
}

/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
static int 
VRReplica::disconnect_and_cleanup()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
       /* Now we wait for the client to send us disconnect event */
       debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_DISCONNECTED, 
		       &cm_event);
       if (ret) {
	       rdma_error("Failed to get disconnect event, ret = %d \n", ret);
	       return ret;
       }
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	printf("A disconnect event is received from the client...\n");
	/* We free all the resources */
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	rdma_buffer_free(server_buffer_mr);
	rdma_buffer_deregister(server_metadata_mr);	
	rdma_buffer_deregister(client_metadata_mr);	
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy rdma server id */
	ret = rdma_destroy_id(cm_server_id);
	if (ret) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Server shut-down is complete \n");
	return 0;
}
	
void
VRReplica::server_send(){
}
	
void
VRReplica::server_receive(){
	memset(dst,0, sizeof(dst));
	memset(type, 0, sizeof(type));
	//ibv_post_recv();
    	memcpy(type, dst, 1);
	switch(*type){
		case 'a':config
		    break;
		case 'A':config
		    break;
		case 'b':index
		    break;
		case 'c':initialize
		    break;
		case 'd':transport
		    break;
		case 'e':nullApp
		    break;
		case 'f':UnloggedRequest
			
		    break;
		case 'g':prepare
			
		    break;
		case 'h':commit
			
		    break;
		case 'i':RequestStateTransfer
		    break;
	        case 'j':StateTransfer
		    break;
	        case 'k':StartViewChange
		    break;
		case 'l':DoViewChange
		    break;
		case 'm':StartView
		    break;
		case 'n':Recovery
		    break;
		case 'o':RecoveryResponse
		    break;
		case 'p':remote
		    break;
		case 'F':UnloggedRequest
			//HandleUnlogged()
		    break;
		case 'G':prepare
			//HandlePrepare()
		    break;
		case 'H':commit
			//
		    break;
		case 'I':RequestStateTransfer
		    break;
	        case 'J':StateTransfer
		    break;
	        case 'K':StartViewChange
		    break;
		case 'L':DoViewChange
		    break;
		case 'M':StartView
		    break;
		case 'N':Recovery
		    break;
		case 'O':RecoveryResponse
		    break;
		case 'P':remote
		    break;
		
	}
}
//make main constantly listening on certain addr and port
int main(int argc, char **argv) 
{
	static struct rdma_event_channel *cm_event_channel = NULL;
	static struct rdma_cm_id *cm_server_id = NULL, *cm_client_id = NULL;
	static struct ibv_pd *pd = NULL;
	static struct ibv_comp_channel *io_completion_channel = NULL;
	static struct ibv_cq *cq = NULL;
	static struct ibv_qp_init_attr qp_init_attr;
	static struct ibv_qp *client_qp = NULL;
	/* RDMA memory resources */
	static struct ibv_mr *client_metadata_mr = NULL, *server_buffer_mr = NULL, *server_metadata_mr = NULL;
	static struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
	static struct ibv_recv_wr client_recv_wr, *bad_client_recv_wr = NULL;
	static struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
	static struct ibv_sge client_recv_sge, server_send_sge;
	static char *src = NULL, *dst = NULL; *type = NULL;
	int ret;
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	const char RDMA_CLIENT_ADDR = 10.1.0.7;
	ret = get_addr(RDMA_CLIENT_ADDR, (struct sockaddr*) &server_sockaddr);
	if (ret) {
		rdma_error("Invalid IP \n");
		return ret;
	}
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	ret = start_rdma_server(&server_sockaddr);
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = setup_client_resources();
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
		return ret;
	}
	ret = accept_client_connection();
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = send_server_metadata_to_client();
	if (ret) {
		rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
		return ret;
	}
	//RDMA is ready, do 6 times of rdma Receive to get constructor input
	//VRReplica(agc =6 );
	//while(true){RDMA receive+ process_work_completion_events()}
	return 0;
}
