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
'a' config+myIdx+transport                                         'a' ack
'b' remote+Unlogged_request                                        'b' CloseBatch--PBMessage(lastPrepare)
'c' remote+Prepare                                                 'c' HandleUnlogged--ToClientMessage m
'd' remote+Commit                                                  'd' HandlePrepare--ToClientMessage m
'e' remote+RequestStateTransfer                                    'e' HandleStateTransfer--lastOp changed
'f' remote+StateTransfer                                           'f' HandleStartViewChange--ToReplicaMessage m
'g' remote+StartViewChange                                         *'g' HandleDoViewChange--lastOp+ToReplicaMessage m
'h' remote+DoViewChange                                            *'h' HandleStartView--lastOp changed+client_receive()
'i' remote+StartView                                               'i' HandleRecovery--ToReplicaMessage m 
'j' remote+Recovery                                                *'j' HandleRecoveryResponse--lastOp changed+client_receive()
'k' remote+RecoveryResponse                                        'k' Latency_Start(&executeAndReplyLatency)
'l' Closebatch                                                     'l' Latency_End(&executeAndReplyLatency)-still in while loop, CommitUpto--transport
'm'                                                               *'m' Latency_End(&executeAndReplyLatency)--while loop end, CommitUpto--transport
X'n'                                                               'n' Latency_End(&executeAndReplyLatency)-still in while loop, NO CommitUpto--transport
X'o'                                                               *'o' Latency_End(&executeAndReplyLatency)--while loop end, NO CommitUpto--transport
X'p'                                                                'p' 
X'q'                                                                'q' sendPrepareOK->transport
'r'                                                                 'r' 
's'                                                                's' RequestStateTransfer()->transport
't' send lastop, batchcomplete=false,                              't' EnterView->Amleader==true (view, stauts, lastBatched, batchcomplete, nullCommitTO->start()), prepareOKQuorum.Clear(); client_receive()
resendPrepareTimeout->Reset();closeBatchTimeout->Stop()            'u' EnterView->Amleader==false (view, stauts, lastBatched, batchcomplete, nullCommitTO->stop, resendPrepareTO->stop, closeBatchTO->stop()), prepareOKQuorum.Clear();, client_receive()
'u' 
'v' NullCOmmitTimeout->start()                                     'v' StartViewChange+view, status, nullCommitTimeout->Stop();resendPrepareTimeout->Stop();closeBatchTimeout->Stop();client_receive()
'w'                                                                'w' StartViewChange, client_receive();
'x'                                                               'x' UpdateClientTable->clienttable
'y'                                                                'y' CloseBatch->transport
'z'                                                                'z' HanldeRequestStateTransfer()->transport
'A'                               
'B' HandleRequest()--clientAddress, updateclienttable(), lastOp, new log entry, nullCommitTimeout->Reset();
'C' HandleRequest()--clientAddress, updateclienttable(), lastOp, new log entry, closeBatchTimeout->Start(), nullCommitTimeout->Reset()
'D' HandlePrepareOk--RequestStateTransfer()
'E' HandlePrepareOk--CommitUpTo(), nullCommitTimeout->Reset();, prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg)

*/
#include "common/replica.h"
#include "replication/vr/replica.h"

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "common/pbmessage.h"
#include "lib/dpdktransport.h"
#include <algorithm>
#include <random>
#include "rdma_common.h"
#include "rdma_server.h"
#define RDebug(fmt, ...) Debug("[%d] " fmt, 0, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, 0, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, 0, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, 0, ##__VA_ARGS__)


//Host Machine should be RDMA server
/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
//write a main, so during experiment, I will also run one process on Node10
	static struct rdma_event_channel *cm_event_channel = NULL;
	static struct rdma_cm_id *cm_server_id = NULL, *cm_client_id = NULL;
	static struct ibv_pd *pd = NULL;
	static struct ibv_comp_channel *io_completion_channel = NULL;
	static struct ibv_cq *cq = NULL;
	static struct ibv_qp_init_attr qp_init_attr;
	static struct ibv_qp *client_qp = NULL;
	/* RDMA memory resources */
	static struct ibv_mr *client_metadata_mr = NULL, *server_metadata_mr = NULL, *server_src_mr = NULL, *server_dst_mr = NULL;
	static struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
	static struct ibv_recv_wr client_recv_wr, *bad_client_recv_wr = NULL;
	static struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
	static struct ibv_sge client_recv_sge, server_send_sge;
	static char *src = NULL, *dst = NULL, *type = NULL;
void
VRReplica(Configuration config, int myIdx,
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
void
delete_VRReplica()
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
    disconnect_and_cleanup();
}

uint64_t
GenerateNonce() const
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);
}

bool
AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}

void
CommitUpTo(opnum_t upto)
{   
    struct ibv_wc wc;
    int timeleft = upto - lastCommitted;
    while (timeleft > 0) {
        Latency_Start(&executeAndReplyLatency);
        memset(src, 'k', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, &wc, 1);
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
		process_work_completion_events(io_completion_channel, &wc, 1);
	    }else{
		memset(src,'m',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, &wc, 1);
	    }
	}else{
	    timeleft--;
	    Latency_End(&executeAndReplyLatency);
	    if (timeleft>0){
		memset(src,'n',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, &wc, 1);
	    }else{
		memset(src,'o',1);
		rdma_server_send();
		process_work_completion_events(io_completion_channel, &wc, 1);
	    }
	}
    }
}

void
SendPrepareOKs(opnum_t oldLastOp)
{
    struct ibv_wc wc;
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
	process_work_completion_events(io_completion_channel, &wc, 1);
	/*   
        if (!(transport->SendMessageToReplica(this,configuration.GetLeaderIndex(view),PBMessage(m)))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
	*/
    }
}


void
RequestStateTransfer()
{
    struct ibv_wc wc;
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
	process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    RNotice("Requesting state transfer: " FMT_VIEWSTAMP, view, lastCommitted);

    this->lastRequestStateTransferView = view;
    this->lastRequestStateTransferOpnum = lastCommitted;
    memset(src, 's', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
    /*
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send RequestStateTransfer message to all replicas");
    }
    */
}

void
EnterView(view_t newview)
{
    struct ibv_wc wc;
    RNotice("Entering new view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_NORMAL;
    lastBatchEnd = lastOp;
    batchComplete = true;
    memcpy(src+1, &view, sizeof(view));
    memcpy(src+1+sizeof(view), &lastBatchEnd, sizeof(lastBatchEnd));
    recoveryTimeout->Stop();

    if (AmLeader()) {
        viewChangeTimeout->Stop();
        nullCommitTimeout->Start();
	memset(src, 't', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, &wc, 1);
    } else {
        viewChangeTimeout->Start();
        nullCommitTimeout->Stop();
        resendPrepareTimeout->Stop();
        closeBatchTimeout->Stop();
	memset(src, 'u', 1);
	rdma_server_send();
	process_work_completion_events(io_completion_channel, &wc, 1);
    }

    prepareOKQuorum.Clear();
    startViewChangeQuorum.Clear();
    doViewChangeQuorum.Clear();
    recoveryResponseQuorum.Clear();
}

void
StartViewChange(view_t newview)
{
    struct ibv_wc wc[2];
    RNotice("Starting view change for view " FMT_VIEW, newview);

    view = newview;
    status = STATUS_VIEW_CHANGE;
    memset(src, 'v', 1);
    memcpy(src+1, &view, sizeof(view));
    viewChangeTimeout->Reset();
    nullCommitTimeout->Stop();
    resendPrepareTimeout->Stop();
    closeBatchTimeout->Stop();
    rdma_server_send();
    ToReplicaMessage m;
    StartViewChangeMessage *svc = m.mutable_start_view_change();
    svc->set_view(newview);
    svc->set_replicaidx(this->replicaIdx);
    svc->set_lastcommitted(lastCommitted);
    memset(src, 'w', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, wc, 2);
    /*
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send StartViewChange message to all replicas");
    }
    */
}


void
UpdateClientTable(const Request &req)
{
    struct ibv_wc wc;
    ClientTableEntry &entry = clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
    memset(src, 'x', 1);
    int sizeofclientTable = sizeof(clientTable);
    memcpy(src+1, &sizeofclientTable, sizeof(int));
    memcpy(src+1+sizeof(int), &clientTable, sizeof(clientTable));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
}

    
void
HandleUnloggedRequest(const TransportAddress &remote,
                                 const UnloggedRequestMessage &msg)
{
    struct ibv_wc wc;
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
    memcpy(src+1+sizeof(m), &remote, sizeof(remote));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
    /*
    if (!(transport->SendMessage(this, remote, PBMessage(m))))
        Warning("Failed to send reply message");
    */
}

void
HandlePrepare(const TransportAddress &remote,
                         const PrepareMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received PREPARE <" FMT_VIEW "," FMT_OPNUM "-" FMT_OPNUM ">",
           msg.view(), msg.batchstart(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPARE due to abnormal status");
        return;
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
        //pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
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
        process_work_completion_events(io_completion_channel, &wc, 1);
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
        //pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
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
    process_work_completion_events(io_completion_channel, &wc, 1);
    /*
    if (!(transport->SendMessageToReplica(this,
                                          configuration.GetLeaderIndex(view),
                                          PBMessage(m)))) {
        RWarning("Failed to send PrepareOK message to leader");
    }
    */
}

    
void
HandleCommit(const TransportAddress &remote,
                        const CommitMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received COMMIT " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring COMMIT due to abnormal status");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if (msg.view() < this->view) {
        RDebug("Ignoring COMMIT due to stale view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if (msg.opnum() > this->lastOp) {
        RequestStateTransfer();
        return;
    }

    CommitUpTo(msg.opnum());
    memset(src, 'a', 1);
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
}


void
HandleRequestStateTransfer(const TransportAddress &remote,
                                      const RequestStateTransferMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received REQUESTSTATETRANSFER " FMT_VIEWSTAMP,
           msg.view(), msg.opnum());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring REQUESTSTATETRANSFER due to abnormal status");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
    memset(src, 'z', 1);
    memcpy(src+1, &m, sizeof(m));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
    //transport->SendMessage(this, remote, PBMessage(m));
}

//need to double check
void
HandleStateTransfer(const TransportAddress &remote,
                               const StateTransferMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received STATETRANSFER " FMT_VIEWSTAMP, msg.view(), msg.opnum());

    if (msg.view() < view) {
        RWarning("Ignoring state transfer for older view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
		memset(src, 'e', 1);
		memcpy(src+1, &lastOp, sizeof(lastOp));
                rdma_server_send();
                process_work_completion_events(io_completion_channel, &wc, 1);
                oldLastOp = lastOp;

                viewstamp_t vs = { newEntry.view(), newEntry.opnum() };
                log.Append(new LogEntry(vs, LOG_STATE_PREPARED, newEntry.request()));
            }
        } else {
            // This is a new operation to us. Add it to the log.
            ASSERT(newEntry.opnum() == lastOp+1);

            lastOp++;
	    memset(src, 'e', 1);
	    memcpy(src+1, &lastOp, sizeof(lastOp));
            rdma_server_send();
            process_work_completion_events(io_completion_channel, &wc, 1);
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

}

void
HandleStartViewChange(const TransportAddress &remote,
                                 const StartViewChangeMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received STARTVIEWCHANGE " FMT_VIEW " from replica %d",
           msg.view(), msg.replicaidx());

    if (msg.view() < view) {
        RDebug("Ignoring STARTVIEWCHANGE for older view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring STARTVIEWCHANGE for current view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
            memset(src, 'f', 1);
	    memcpy(src+1, &m, sizeof(m));
            rdma_server_send();
            process_work_completion_events(io_completion_channel, &wc, 1);
	    /*
            if (!(transport->SendMessageToReplica(this, leader, PBMessage(m)))) {
                RWarning("Failed to send DoViewChange message to leader of new view");
            }
	    */
        }
    }
}


void
HandleDoViewChange(const TransportAddress &remote,
                              const DoViewChangeMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received DOVIEWCHANGE " FMT_VIEW " from replica %d, "
           "lastnormalview=" FMT_VIEW " op=" FMT_OPNUM " committed=" FMT_OPNUM,
           msg.view(), msg.replicaidx(),
           msg.lastnormalview(), msg.lastop(), msg.lastcommitted());

    if (msg.view() < view) {
        RDebug("Ignoring DOVIEWCHANGE for older view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RDebug("Ignoring DOVIEWCHANGE for current view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
	memset(src, 'g', 1);
	memcpy(src+1, &lastOp, sizeof(lastOp));    
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
        memcpy(src+1+sizeof(lastOp), &m, sizeof(m));
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
	/*
        if (!(transport->SendMessageToAll(this, PBMessage(m)))) {
            RWarning("Failed to send StartView message to all replicas");
        }
	*/
    }
}

void
HandleStartView(const TransportAddress &remote,
                           const StartViewMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received STARTVIEW " FMT_VIEW
          " op=" FMT_OPNUM " committed=" FMT_OPNUM " entries=%d",
          msg.view(), msg.lastop(), msg.lastcommitted(), msg.entries_size());
    RDebug("Currently in view " FMT_VIEW " op " FMT_OPNUM " committed " FMT_OPNUM,
          view, lastOp, lastCommitted);

    if (msg.view() < view) {
        RWarning("Ignoring STARTVIEW for older view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
        RWarning("Ignoring STARTVIEW for current view");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
    memset(src, 'h', 1);
    memcpy(src+1, &lastOp, sizeof(lastOp));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
    ASSERT(!AmLeader());

    CommitUpTo(msg.lastcommitted());
    SendPrepareOKs(oldLastOp);
    
}

void
HandleRecovery(const TransportAddress &remote,
                          const RecoveryMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received RECOVERY from replica %d", msg.replicaidx());

    if (status != STATUS_NORMAL) {
        RDebug("Ignoring RECOVERY due to abnormal status");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
    memset(src, 'i', 1);
    memcpy(src+1, &m, sizeof(m));
    memcpy(src+1+sizeof(m), &remote, sizeof(remote));
    rdma_server_send();
    process_work_completion_events(io_completion_channel, &wc, 1);
    /*
    if (!(transport->SendMessage(this, remote, PBMessage(m)))) {
        RWarning("Failed to send recovery response");
    }
    */
    return;
}

void
HandleRecoveryResponse(const TransportAddress &remote,
                                  const RecoveryResponseMessage &msg)
{
    struct ibv_wc wc;
    RDebug("Received RECOVERYRESPONSE from replica %d",
           msg.replicaidx());

    if (status != STATUS_RECOVERING) {
        RDebug("Ignoring RECOVERYRESPONSE because we're not recovering");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if (msg.nonce() != recoveryNonce) {
        RNotice("Ignoring recovery response because nonce didn't match");
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
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
	    memset(src, 'a', 1);
            rdma_server_send();
            process_work_completion_events(io_completion_channel, wc, 1);
            return;
        }

        Notice("Recovery completed");

        log.Install(leaderResponse->second.entries().begin(),
                    leaderResponse->second.entries().end());
        EnterView(leaderResponse->second.view());
        lastOp = leaderResponse->second.lastop();
	memset(src, 'j', 1);
	memcpy(src+1, &lastOp, sizeof(lastOp));
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
        CommitUpTo(leaderResponse->second.lastcommitted());
	memset(src, 'a', 1);
        rdma_server_send();
        process_work_completion_events(io_completion_channel, &wc, 1);
    }
}

//Below are for RDMA server
	
void
rdma_server_send()
{
	struct ibv_wc wc;
	
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	server_send_sge.addr = (uint64_t) server_src_mr->addr;
	server_send_sge.length = (uint32_t) server_src_mr->length;
	server_send_sge.lkey = server_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&server_send_wr, sizeof(server_send_wr));
	server_send_wr.sg_list = &server_send_sge;
	server_send_wr.num_sge = 1;
	server_send_wr.opcode = IBV_WR_SEND;
	server_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	//client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	//client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	/* Now we post it */
	ibv_post_send(client_qp, 
		       &server_send_wr /* Send request that we prepared before */, 
		       &bad_server_send_wr);
	
	memset(src, 0, sizeof(src));
}
	
void
rdma_server_receive()
{
	struct ibv_wc wc;
	memset(dst,0, sizeof(dst));
	memset(type, 0, sizeof(type));
	/* Now we prepare a READ using same variables but for destination */
	client_recv_sge.addr = (uint64_t) server_dst_mr->addr;
	client_recv_sge.length = (uint32_t) server_dst_mr->length;
	client_recv_sge.lkey = server_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_recv_wr, sizeof(client_recv_wr));
	client_recv_wr.sg_list = &client_recv_sge;
	client_recv_wr.num_sge = 1;
	/* Now we post it */
	ibv_post_recv(client_qp, 
		       &client_recv_wr,
	       &bad_client_recv_wr);
	
	// at this point we are expecting 1 work completion for the write 
	//leave process_work_completion_events()
	debug("Client side receive is complete \n");
	
	
	//ibv_post_recv();
    	memcpy(type, dst, 1);
	switch(*type){
		case 'a':{//config+myIdx+initialize+transport+nullApp
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    dsnet::Configuration *config = NULL;
		    int *myIdx = NULL;
		    std::string transport_cmdline;
		    dsnet::Transport *transportptr = new dsnet::DPDKTransport(0, 0, 1, 0, transport_cmdline);
		    memcpy(config, dst+1, sizeof(*config));
		    memcpy(myIdx, dst+1+sizeof(*config), sizeof(int));
		    break;
		}
		case 'b':{//remote+Unlogged_request
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.unlogged_request(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.unlogged_request()));
		    HandleUnloggedRequest(remote, replica_msg.unlogged_request());
		    break;
		}
		case 'c':{//remote+Prepare
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.prepare(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.prepare()));
		    HandlePrepare(remote, replica_msg.prepare());
		    break;
		}
		case 'd':{//remote+Commit
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.commit(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.commit()));
		    HandleCommit(remote, replica_msg.commit());
		    break;
		}
		case 'e':{//remote+RequestStateTransfer
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.request_state_transfer(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.request_state_transfer()));
		    HandleRequestStateTransfer(remote,replica_msg.request_state_transfer());
		    break;
		}
		case 'f':{//remote+StateTransfer
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.state_transfer(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.state_transfer()));
		    HandleStateTransfer(remote, replica_msg.state_transfer());
		    break;
		}
		case 'g':{//remote+StartViewChange
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.start_view_change(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.start_view_change()));
		    HandleStartViewChange(remote, replica_msg.start_view_change());
		    break;
		}
		case 'h':{//remote+DoViewChange
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&(replica_msg.do_view_change(), dst+1+sizeof(TransportAddress), sizeof((replica_msg.do_view_change()));
		    HandleDoViewChange(remote, replica_msg.do_view_change());
		    break;
		}
		case 'i':{//remote+StartView
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.start_view(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.start_view()));
		    HandleStartView(remote, replica_msg.start_view());
		    break;
		}
		case 'j':{//remote+Recovery 
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.recovery(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.recovery()));
		    HandleRecovery(remote, replica_msg.recovery());
		    break;
		}
		case 'k':{//remote+RecoveryResponse 
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    TransportAddress remote;
		    ToReplicaMessage replica_msg;
		    memcpy(&remote, dst+1, sizeof(TransportAddress));
		    memcpy(&replica_msg.recovery_response(), dst+1+sizeof(TransportAddress), sizeof(replica_msg.recovery_response()));
		    HandleRecoveryResponse(remote, replica_msg.recovery_response());
		    break;
		}
		case 'l':{//Closebatch 
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    opnum_t batchStart;
		    memcpy(&batchStart, dst+1, sizeof(opnum_t));
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
    		    process_work_completion_events(io_completion_channel, &wc, 1);
		    break;
		}
		case 't':{//send lastop, batchcomplete=false
			//resendPrepareTimeout->Reset();closeBatchTimeout->Stop()
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    resendPrepareTimeout->Reset();
   		    closeBatchTimeout->Stop();
		    batchComplete = false;
		    memcpy(&lastOp, dst+1, sizeof(lastOp));
		    lastBatchEnd = lastOp;
		    break;
		}
		case 'v':{//NullCOmmitTimeout->start()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    nullCommitTimeout->Start();
		    break;
		}
		case 'B':{//HandleRequest()--clientAddress, updateclienttable(), 
			//lastOp, new log entry, nullCommitTimeout->Reset();
		    nullCommitTimeout->Reset();
		    process_work_completion_events(io_completion_channel, wc, 1);
		    int size;
		    Request req;
		    LogEntry* newlogentry;
		    memcpy(&size, dst+1, sizeof(int));
		    memcpy(&clientAddresses, dst+1+sizeof(int), size);
		    memcpy(&req, dst+1+sizeof(int)+size, sizeof(Request));
		    UpdateClientTable(req);
	            memcpy(&lastOp, dst+1+sizeof(int)+size+sizeof(Request), sizeof(lastOp));
		    memcpy(newlogentry, dst+1+sizeof(int)+size+sizeof(Request)+sizeof(lastOp), sizeof(LogEntry));
		    log.Append(newlogentry);
		    break;
		}
		case 'C':{//HandleRequest()--clientAddress, updateclienttable(),
			//lastOp, new log entry, closeBatchTimeout->Start(), nullCommitTimeout->Reset()
		    closeBatchTimeout->Start();
		    nullCommitTimeout->Reset();
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    int size;
		    Request req;
		    LogEntry newlogentry;
		    memcpy(&size, dst+1, sizeof(int));
		    memcpy(&clientAddresses, dst+1+sizeof(int), size);
		    memcpy(&req, dst+1+sizeof(int)+size, sizeof(Request));
		    UpdateClientTable(req);
	            memcpy(&lastOp, dst+1+sizeof(int)+size+sizeof(Request), sizeof(lastOp));
		    memcpy(&newlogentry, dst+1+sizeof(int)+size+sizeof(Request)+sizeof(lastOp), sizeof(LogEntry));
		    log.Append(newlogentry);
		    break;
		}
		case 'D':{//HandlePrepareOk--RequestStateTransfer()
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    RequestStateTransfer();
		    break;
		}
		case 'E':{//HandlePrepareOk--CommitUpTo(), nullCommitTimeout->Reset();
			//prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg))
		    nullCommitTimeout->Reset();
		    process_work_completion_events(io_completion_channel, &wc, 1);
		    viewstamp_t vs;
		    PrepareOKMessage msg;
		    memcpy(&vs, dst+1, sizeof(viewstamp_t));
		    memcpy(&msg, dst+1+sizeof(viewstamp_t), sizeof(msg));
		    prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg);
		    CommitUpTo(msg.opnum());
		    memset(src, 'a', 1);
    		    rdma_server_send();
    		    process_work_completion_events(io_completion_channel, &wc, 1);
		    break;
		}
		
	}
}
//make main constantly listening on certain addr and port
int main(int argc, char **argv) 
{
	int ret;
	std::string transport_cmdline;
	dsnet::AppReplica *nullApp = new dsnet::AppReplica();
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	char* const RDMA_CLIENT_ADDR = "10.1.0.7";
	src = dst = type = NULL;
        src = (char *)calloc(1073741824,1); 
        dst = (char *)calloc(1073741824,1); //hardcoded every RDMA read and for 1 GB (MAX Capacity is 2GB), 
        type = (char *)calloc(sizeof(char),1);
	ret = dsnet::vr::get_addr(RDMA_CLIENT_ADDR, (struct sockaddr*) &server_sockaddr);
	if (ret) {
		rdma_error("Invalid IP \n");
		return ret;
	}
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	ret = dsnet::vr::start_rdma_server(&server_sockaddr);
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = dsnet::vr::setup_client_resources();
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
		return ret;
	}
	ret = dsnet::vr::accept_client_connection();
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = dsnet::vr::send_server_metadata_to_client();
	if (ret) {
		rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
		return ret;
	}
	dsnet::vr::rdma_server_receive();
	
	VR = new dsnet::vr::VRReplica(config, myIdx, true, transportptr, 1, nullApp);
	while(true){
		VR.rdma_server_receive();
	}
	return 0;
}
