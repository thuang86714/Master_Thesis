//replica logic on BF should be as lightweight as possible. All the slow path logic should be executed on Node10
//We want to offload the extra tasks of a leader replica to BF, for example Send N Prepare messgare && Receive/Process N PrepareOK)
/*
 * The RDMA server client part of code. 
 *
 * Author: Animesh Trivedi 
 *         atrivedi@apache.org 
 *
 * TODO: Cleanup previously allocated resources in case of an error condition
 */
/*
from client to server           from server to client
'a' config                      'Q' config
'b' myIdx                       'R' myIdx
'c' initialize                  'S' view-num
'd' transport                   'T' status
'e' nullApp                     'U' op-num
'f' UnloggedRequest             'V' log
'g' Prepare                     'W' commit-num
'h' commit                      'X' client-table
'i' RequestStateTransfer        'Y' ack
'j' StateTransfer
'k' StartViewChange
'l' DoViewChange
'm' StartView
'n' Recovery
'o' RecoveryResponse
'p' Remote
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

//the next two lib are for RDMA
#include "rdma_common.h"

namespace dsnet {
namespace vr {

using namespace proto;
	
//BF will be used as RDMA client, the following 20 lines are for RDMA Client Resource init.
/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
//Hardcoded the RDMA server addr as 10.1.0.4
	//Need to find way for sent message other than string
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *client_cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp;
/* These are memory buffers related resources */
static struct ibv_mr *client_metadata_mr = NULL, 
		     *client_src_mr = NULL, 
		     *client_dst_mr = NULL, 
		     *server_metadata_mr = NULL;
static struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
static struct ibv_send_wr client_send_wr, *bad_client_send_wr = NULL;
static struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
static struct ibv_sge client_send_sge, server_recv_sge;
/* Source and Destination buffers, where RDMA operations source and sink */
static char *src = NULL, *dst = NULL; 
	
  
//for constrcutor, should have a RDMA write function to write initial state to RDMA server(the host)
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
    _Latency_Init(&executeAndReplyLatency, "executeAndReply";
//add a rdma write function (for registration propose) to RDMA server. 
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
	//bellow are for RDMA client
	// Hard-coded the destination address
	const char RDMA_SERVER_ADDR = 10.1.0.4;
	struct sockaddr_in server_sockaddr;
	int ret;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	/* buffers are NULL */
	src = dst = NULL; 
	src = (char *)calloc(1073741824,1); //=1GB, would that cause overflow? Nope(Q1
	dst = (char *)calloc(1073741824,1); //hardcoded every RDMA read and for 1 GB (MAX Capacity is 2GB), 
        //would this amount of capacity affect performance?
	//set address
	ret = get_addr(RDMA_SERVER_ADDR, (struct sockaddr*) &server_sockaddr);
	if (ret) {
					rdma_error("Invalid IP \n");
					return ret;
				}
	//set to default port
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
	///* This function prepares client side connection resources for an RDMA connection */
	ret = client_prepare_connection(&server_sockaddr);
	if (ret) { 
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	 }
	/* Pre-posts a receive buffer before calling rdma_connect () */
	ret = client_pre_post_recv_buffer(); 
	if (ret) { 
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	/* Connects to the RDMA server */
	ret = client_connect_to_server();
	if (ret) { 
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	//Exchange buffer metadata with the server.
	ret = client_xchange_metadata_with_server();
	if (ret) {
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	int ret = -1;
	client_dst_mr = rdma_buffer_register(pd,
			dst,
			strlen(src),
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	if (!client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	//RDMA write for registration; (Configuration config, int myIdx,bool initialize,
	//Transport *transport, int batchSize(will hard-coded this one as 0),AppReplica *app)
	//send config
        memset(src, 0, sizeof(src));
	memset(src, 'a', 1);
	memcpy(src+1, &config, sizeof(config));
	client_send();
        client_receive();
        ret = process_work_completion_events(io_completion_channel, wc, 2);
	//send myIdx
	memset(src, 'b', 1);
	memcpy(src+1, &myIdx, sizeof(myIdx));
	client_send();
	client_receive();
	ret = process_work_completion_events(io_completion_channel, wc, 2);
	//send initialize
	memset(src, 'c', 1);
        memcpy(src+1, &initialize, sizeof(initialize));
	client_send();
	client_receive();
	ret = process_work_completion_events(io_completion_channel, wc, 2);
	//send transport
	memset(src, 'd', 1);
        memcpy(src+1, transport, sizeof(*transport)); //dereference transport
        client_send();
	client_receive();
	ret = process_work_completion_events(io_completion_channel, wc, 2);
        //Hard-coding Batchsize
	//send app
	memset(src, 'e', 1);
        memcpy(src+1, app, sizeof(*app));//dereference app
        client_send();
	client_recieve();
	ret = process_work_completion_events(io_completion_channel, wc, 2);
	    
		  
}

		
//destructor should not need any change
VRReplica::~VRReplica()
{
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    delete viewChangeTimeout;
    delete nullCommitTimeout;
    delete stateTransferTimeout;
    delete resendPrepareTimeout;
    delete closeBatchTimeout;
    delete recoveryTimeout;

    for (auto &kv : pendingPrepares) {
        delete kv.first;
    }
    client_disconnect_and_clean();
}

//fast-path && non-leader
bool
VRReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}		  
		  
//send prepare message
//No need tidy up
void
VRReplica::CloseBatch()
{
    ASSERT(AmLeader());
    ASSERT(lastBatchEnd < lastOp);

    opnum_t batchStart = lastBatchEnd+1;

    RDebug("Sending batched prepare from " FMT_OPNUM
           " to " FMT_OPNUM,
           batchStart, lastOp);
    /* Send prepare messages */
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

    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        RWarning("Failed to send prepare message to all replicas");
    }
    lastBatchEnd = lastOp;
    batchComplete = false;

    resendPrepareTimeout->Reset();
    closeBatchTimeout->Stop();
}
  


void
VRReplica::ResendPrepare()
{
    ASSERT(AmLeader());
    if (lastOp == lastCommitted) {
        return;
    }
    RNotice("Resending prepare");
    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        RWarning("Failed to ressend prepare message to all replicas");
    }
}
  

//further divide needed
void
VRReplica::ReceiveMessage(const TransportAddress &remote,
                          void *buf, size_t size)
{
    static ToReplicaMessage replica_msg;
    static PBMessage m(replica_msg);

    m.Parse(buf, size);
    switch (replica_msg.msg_case()) {
        case ToReplicaMessage::MsgCase::kRequest:
            //HandleRequest is leader-replica-specific task, should remain on BF 
            HandleRequest(remote, replica_msg.request());
            break;
	case ToReplicaMessage::MsgCase::kPrepareOk:
            //HandleRequest is leader-replica-specific task, should remain on BF 
            HandlePrepareOK(remote, replica_msg.prepare_ok());
	    break;
	//all cases below should be executed on the Host
        case ToReplicaMessage::MsgCase::kUnloggedRequest:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleUnloggedRequest(remote, replica_msg.unlogged_request());
	    //send remote
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //send unlog
	    memset(src, 'f', 1);
	    memcpy(src+1, replica_msg.unlogged_request(), sizeof(replica_msg.unlogged_request()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kPrepare:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandlePrepare(remote, replica_msg.prepare());
	    //send remote
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //send prepare
	    memset(src, 'g', 1);
	    memcpy(src+1, replica_msg.prepare(), sizeof(replica_msg.prepare()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kCommit:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleCommit(remote, replica_msg.commit());
	    //send remote
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //send commit
	    memset(src, 'h', 1);
	    memcpy(src+1, replica_msg.commit(), sizeof(replica_msg.commit()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kRequestStateTransfer:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleRequestStateTransfer(remote,replica_msg.request_state_transfer());
	    //send remote
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //send request_state_transfer
	    memset(src, 'i', 1);
	    memcpy(src+1, replica_msg.request_state_transfer(), sizeof(replica_msg.request_state_transfer()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kStateTransfer:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleStateTransfer(remote, replica_msg.state_transfer());
	    //send remote
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    memset(src, 'j', 1);
	    memcpy(src+1, replica_msg.state_transfer(), sizeof(replica_msg.state_transfer()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kStartViewChange:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleStartViewChange(remote, replica_msg.start_view_change());
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //send start_view_change
	    memset(src, 'k', 1);
	    memcpy(src+1, replica_msg.start_view_change(), sizeof(replica_msg.start_view_change()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kDoViewChange:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleDoViewChange(remote, replica_msg.do_view_change());
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //there's problem here, how to do server side notification when write is done
	    memset(src, 'l', 1);
	    memcpy(src+1, replica_msg.do_view_change(), sizeof(replica_msg.do_view_change()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kStartView:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleStartView(remote, replica_msg.start_view());
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    cclient_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //there's problem here, how to do server side notification when write is done
	    memset(src, 'm', 1);
	    memcpy(src+1, replica_msg.start_view(), sizeof(replica_msg.start_view()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kRecovery:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleRecovery(remote, replica_msg.recovery());
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //there's problem here, how to do server side notification when write is done
	    memset(src, 'n', 1);
	    memcpy(src+1, replica_msg.recovery(), sizeof(replica_msg.recovery()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        case ToReplicaMessage::MsgCase::kRecoveryResponse:
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleRecoveryResponse(remote, replica_msg.recovery_response());
	    memset(src, 'p', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
	    //there's problem here, how to do server side notification when write is done
	    memset(src, 'o', 1);
	    memcpy(src+1, replica_msg.recovery_response(), sizeof(replica_msg.recovery_response()));
	    client_send();
	    client_recieve();
	    ret = process_work_completion_events(io_completion_channel, wc, 2);
            break;
        default:
            //the line below should not need further change
            RPanic("Received unexpected message type %u",
                    replica_msg.msg_case());
    }
}

void
VRReplica::HandleRequest(const TransportAddress &remote,
                         const RequestMessage &msg)
{
    viewstamp_t v;
    Latency_Start(&requestLatency);

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status");
        Latency_EndType(&requestLatency, 'i');
        return;
    }

    if (!AmLeader()) {
        RDebug("Ignoring request because I'm not the leader");
        Latency_EndType(&requestLatency, 'i');
        return;
    }

    // Save the client's address
    clientAddresses.erase(msg.req().clientid());
    clientAddresses.insert(
        std::pair<uint64_t, std::unique_ptr<TransportAddress> >(
            msg.req().clientid(),
            std::unique_ptr<TransportAddress>(remote.clone())));

    // Check the client table to see if this is a duplicate request
    auto kv = clientTable.find(msg.req().clientid());
    if (kv != clientTable.end()) {
        ClientTableEntry &entry = kv->second;
        if (msg.req().clientreqid() < entry.lastReqId) {
            RNotice("Ignoring stale request");
            Latency_EndType(&requestLatency, 's');
            return;
        }
        if (msg.req().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply if we
            // have one. We might not have a reply to resend if we're
            // waiting for the other replicas; in that case, just
            // discard the request.
            if (entry.replied) {
                RNotice("Received duplicate request; resending reply");
                if (!(transport->SendMessage(this, remote,
                                             PBMessage(entry.reply)))) {
                    RWarning("Failed to resend reply to client");
                }
                Latency_EndType(&requestLatency, 'r');
                return;
            } else {
                RNotice("Received duplicate request but no reply available; ignoring");
                Latency_EndType(&requestLatency, 'd');
                return;
            }
        }
    }
    
    // Update the client table
    UpdateClientTable(msg.req());
    memset(src, 0, sizeof(src));
    memcpy(ct, msg.req(), sizeof(msg.req()));
    client_rdma_remote_write();

    // Leader Upcall
    bool replicate = false;
    string res;
    LeaderUpcall(lastCommitted, msg.req().op(), replicate, res);
    ClientTableEntry &cte =
        clientTable[msg.req().clientid()];

    // Check whether this request should be committed to replicas
    if (!replicate) {
        RDebug("Executing request failed. Not committing to replicas");
        ToClientMessage m;
        ReplyMessage *reply = m.mutable_reply();

        reply->set_reply(res);
        reply->set_view(0);
        reply->set_opnum(0);
        reply->set_clientreqid(msg.req().clientreqid());
        cte.replied = true;
        cte.reply = m;
        transport->SendMessage(this, remote, PBMessage(m));
        Latency_EndType(&requestLatency, 'f');
    } else {
        Request request;
        request.set_op(res);
        request.set_clientid(msg.req().clientid());
        request.set_clientreqid(msg.req().clientreqid());

        /* Assign it an opnum */
        ++this->lastOp;
        v.view = this->view;
        v.opnum = this->lastOp;

        RDebug("Received REQUEST, assigning " FMT_VIEWSTAMP, VA_VIEWSTAMP(v));

        /* Add the request to my log */
        log.Append(new LogEntry(v, LOG_STATE_PREPARED, request));

        if (batchComplete ||
            (lastOp - lastBatchEnd+1 > (unsigned int)batchSize)) {
            CloseBatch();
        } else {
            RDebug("Keeping in batch");
            if (!closeBatchTimeout->Active()) {
                closeBatchTimeout->Start();
            }
        }

        nullCommitTimeout->Reset();
        Latency_End(&requestLatency);
    }
}

		  
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
	    //Require a RDMA write
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
         */
	    
	//the line below require RDMA write to N10. No need do RDMA read since the return of CommitUpto is void.
        //CommitUpTo(msg.opnum());
	memset(src, 0, sizeof(src));
	memcpy(src, msg.opnum(), sizeof(msg.opnum()));
	client_rdma_remote_write();//there's problem here, server side notification
	client_rdma_remote_read(); //update the state on BF

        if (msgs->size() >= (unsigned int)configuration.QuorumSize()) {
            return;
        }

        /*
         * Send COMMIT message to the other replicas.
         *
         * This can be done asynchronously, so it really ought to be
         * piggybacked on the next PREPARE or something.
         */
        ToReplicaMessage m;
        CommitMessage *c = m.mutable_commit();
        c->set_view(this->view);//State Sync
        c->set_opnum(this->lastCommitted);//State Sync

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

static int client_prepare_connection(struct sockaddr_in *s_addr)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return -errno;
	}
	debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_client_id, 
			NULL,
			RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		return -errno;
	}
	/* Resolve destination and optional source addresses from IP addresses  to
	 * an RDMA address.  If successful, the specified rdma_cm_id will be bound
	 * to a local device. */
	ret = rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		return -errno;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
	ret  = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ADDR_RESOLVED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
		return -errno;
	}
	debug("RDMA address is resolved \n");

	 /* Resolves an RDMA route to the destination address in order to 
	  * establish a connection */
	ret = rdma_resolve_route(cm_client_id, 2000);
	if (ret) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return -errno;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ROUTE_RESOLVED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
		return -errno;
	}
	printf("Trying to connect to server at : %s port: %d \n", 
			inet_ntoa(s_addr->sin_addr),
			ntohs(s_addr->sin_port));
	/* Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd = ibv_alloc_pd(cm_client_id->verbs);
	if (!pd) {
		rdma_error("Failed to alloc pd, errno: %d \n", -errno);
		return -errno;
	}
	debug("pd allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		rdma_error("Failed to create IO completion event channel, errno: %d\n",
			       -errno);
	return -errno;
	}
	debug("completion event channel created at : %p \n", io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	client_cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!client_cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		return -errno;
	}
	debug("CQ created at %p with %d elements \n", client_cq, client_cq->cqe);
	ret = ibv_req_notify_cq(client_cq, 0);
	if (ret) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
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
       qp_init_attr.recv_cq = client_cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = client_cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
	if (ret) {
		rdma_error("Failed to create QP, errno: %d \n", -errno);
	       return -errno;
	}
	client_qp = cm_client_id->qp;
	debug("QP created at %p \n", client_qp);
	return 0;
}				  
		  
static int client_pre_post_recv_buffer()
{
	int ret = -1;
	server_metadata_mr = rdma_buffer_register(pd,
			&server_metadata_attr,
			sizeof(server_metadata_attr),
			(IBV_ACCESS_LOCAL_WRITE));
	if(!server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) server_metadata_mr->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	debug("Receive buffer pre-posting is successful \n");
	return 0;
}
		  
/* Connects to the RDMA server */
static int client_connect_to_server() 
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3; // if fail, then how many times to retry
	ret = rdma_connect(cm_client_id, &conn_param);
	if (ret) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return -errno;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_ESTABLISHED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, ret = %d \n", ret);
	       return ret;
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		return -errno;
	}
	printf("The client is connected successfully \n");
	return 0;
}
		  
/* Exchange buffer metadata with the server. The client sends its, and then receives
 * from the server. The client-side metadata on the server is _not_ used because
 * this program is client driven. But it shown here how to do it for the illustration
 * purposes
 */
static int client_xchange_metadata_with_server()
{
	struct ibv_wc wc[2];
	int ret = -1;
	client_src_mr = rdma_buffer_register(pd,
			src,
			sizeof(src), //orginal code was strlen()
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!client_src_mr){
		rdma_error("Failed to register the first buffer, ret = %d \n", ret);
		return ret;
	}
	/* we prepare metadata for the first buffer */
	client_metadata_attr.address = (uint64_t) client_src_mr->addr; 
	client_metadata_attr.length = client_src_mr->length; 
	client_metadata_attr.stag.local_stag = client_src_mr->lkey;
	/* now we register the metadata memory */
	client_metadata_mr = rdma_buffer_register(pd,
			&client_metadata_attr,
			sizeof(client_metadata_attr),
			IBV_ACCESS_LOCAL_WRITE);
	if(!client_metadata_mr) {
		rdma_error("Failed to register the client metadata buffer, ret = %d \n", ret);
		return ret;
	}
	/* now we fill up SGE */
	client_send_sge.addr = (uint64_t) client_metadata_mr->addr;
	client_send_sge.length = (uint32_t) client_metadata_mr->length;
	client_send_sge.lkey = client_metadata_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_SEND;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(io_completion_channel, 
			wc, 2);
	if(ret != 2) {
		rdma_error("We failed to get 2 work completions , ret = %d \n",
				ret);
		return ret;
	}
	debug("Server sent us its buffer location and credentials, showing \n");
	show_rdma_buffer_attr(&server_metadata_attr);
	return 0;
}

//orignially this function include 1st RDMA Write from client to server + 2nd RDMA Read from client to server (execute as ordered)
//I will divide the function into 2 separate function, RDMA Write & RDMA Read
//Actually there are 2 approaches, 1 is frequent write(to override), 1 is atomic operation. 
//But according to Page 106, Mellanox Verbs Programming Tutorial (by Dotan Barak) , atomic operation is performance killer
static int client_send() 
{
	struct ibv_wc wc;
	
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	client_send_sge.addr = (uint64_t) client_src_mr->addr;
	client_send_sge.length = (uint32_t) client_src_mr->length;
	client_send_sge.lkey = client_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	client_send_wr.opcode = IBV_WR_SEND;
	client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	//client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	//client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		rdma_error("Failed to send client src buffer, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	debug("Client side SEND is complete \n");
}

//this function is RDMA read: this function could only do Memory Region Level Read, can not do 
static int client_receive()
{
	
	int ret = -1;
	client_dst_mr = rdma_buffer_register(pd,
			dst,
			strlen(src),
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	if (!client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	/* Now we prepare a READ using same variables but for destination */
	client_send_sge.addr = (uint64_t) client_dst_mr->addr;
	client_send_sge.length = (uint32_t) client_dst_mr->length;
	client_send_sge.lkey = client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&client_send_wr, sizeof(client_send_wr));
	client_send_wr.sg_list = &client_send_sge;
	client_send_wr.num_sge = 1;
	//client_send_wr.opcode = IBV_WR_RDMA_READ;
	//client_send_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	//client_send_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	//client_send_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	/* Now we post it */
	ret = ibv_post_rcv(client_qp, 
		       &client_send_wr,
	       &bad_client_send_wr);
	if (ret) {
		rdma_error("Failed to receive client dst buffer from the server, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	debug("Client side receive is complete \n");
	switch(){
		case '10000':config
		    break;
		case '10001':index
		    break;
		case '10010':view-num
		    break;
		case '10100':status
		    break;
		case '11000':op-num
		    break;
		case '11001':log
		    break;
		case '11010':commit-num
		    break;
		case '11100':client-num
		    break;
		case '11111':ack
		    break;
		
	}
	return 0;
} 

		  
/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
static int client_disconnect_and_clean()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(cm_client_id);
	if (ret) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		//continuing anyways
	}
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_DISCONNECTED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
				ret);
		//continuing anyways 
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		//continuing anyways
	}
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(client_cq);
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
	rdma_buffer_deregister(server_metadata_mr);
	rdma_buffer_deregister(client_metadata_mr);	
	rdma_buffer_deregister(client_src_mr);	
	rdma_buffer_deregister(client_dst_mr);	
	/* We free the buffers */
	free(src);
	free(dst);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Client resource clean up is complete \n");
	return 0;
}
