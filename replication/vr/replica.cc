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
//TODO-Tommy 1.move all transport->sendMessagetoAll() function to BF. 2.Edit client_send(), client_receive() to verb based, 
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
#include <string.h>
#include <stdio.h>
//the next two lib are for RDMA
#include "rdma_common.h"
#include "rdma_client.h"
namespace dsnet {
namespace vr {

using namespace proto;
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
    static char *src = NULL, *dst = NULL, *type = NULL;
    this->status = STATUS_NORMAL;
    this->view = 0;
    this->lastOp = 0;
    this->lastCommitted = 0;
    this->lastRequestStateTransferView = 0;
    this->lastRequestStateTransferOpnum = 0;
    lastBatchEnd = 0;
    batchComplete = true;
    Amleader = true; //hard-coded bool of Amleader() function, use RDMA to chage value;
    if (batchSize > 1) {
        Notice("Batching enabled; batch size %d", batchSize);
    }
    //add a rdma write function (for registration propose) to RDMA server. 
    //bellow are for RDMA client
    // Hard-coded the destination address
    char* RDMA_SERVER_ADDR = "10.1.0.4";
    struct sockaddr_in server_sockaddr;
    bzero(&server_sockaddr, sizeof server_sockaddr);
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    /* buffers are NULL */
    src = dst = type = NULL; 
    src = (char *)calloc(1073741824,1); //=1GB, would that cause overflow? Nope(Q1
    dst = (char *)calloc(1073741824,1); //hardcoded every RDMA read and for 1 GB (MAX Capacity is 2GB), 
    type = (char *)calloc(sizeof(char),1);
    //would this amount of capacity affect performance
    //set address
    get_addr(RDMA_SERVER_ADDR, (struct sockaddr*) &server_sockaddr);
    //set to default port
    server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
    ///* This function prepares client side connection resources for an RDMA connection */
    client_prepare_connection(&server_sockaddr);
    /* Pre-posts a receive buffer before calling rdma_connect () */
    client_pre_post_recv_buffer(); 
    /* Connects to the RDMA server */
    client_connect_to_server();
    //Exchange buffer metadata with the server.
    client_xchange_metadata_with_server();
    client_dst_mr = rdma_buffer_register(pd,
		dst,
		sizeof(src),
		(IBV_ACCESS_LOCAL_WRITE | 
		 IBV_ACCESS_REMOTE_WRITE | 
		 IBV_ACCESS_REMOTE_READ));
    //RDMA write for registration; (Configuration config, int myIdx,bool initialize,
    //Transport *transport, int batchSize(will hard-coded this one as 0),AppReplica *app)
    //send config
    struct ibv_wc wc[2];
    memset(src, 0, sizeof(src));
    memset(src, 'a', 1);
    memcpy(src+1, &config, sizeof(config));
    //copy myIdx
    memcpy(src+1+sizeof(config), &myIdx, sizeof(myIdx));
    //copy *transport
    memcpy(src+1+sizeof(config)+sizeof(myIdx), transport, sizeof(*transport)); //dereference transport
    rdma_client_send();
    rdma_client_receive();
    process_work_completion_events(io_completion_channel, wc, 2);
    /* Move these 2 Timeout to N10
    this->viewChangeTimeout = new Timeout(transport, 5000, [this,myIdx]() {
            RWarning("Have not heard from leader; starting view change");
            StartViewChange(view+1);
        });
    this->stateTransferTimeout = new Timeout(transport, 1000, [this]() {
            this->lastRequestStateTransferView = 0;
            this->lastRequestStateTransferOpnum = 0;
        });
    this->stateTransferTimeout->Start();
    */
    //the rest 3 Timeout are actually also part of logic on N10, but I will solve it by RDMA communication.
    this->recoveryTimeout = new Timeout(transport, 5000, [this]() {
            SendRecoveryMessages();
        });
    this->nullCommitTimeout = new Timeout(transport, 1000, [this]() {
            SendNullCommit();
        });
    this->resendPrepareTimeout = new Timeout(transport, 500, [this]() {
            ResendPrepare();
        });
    this->closeBatchTimeout = new Timeout(transport, 300, [this]() {
            CloseBatch();
        });
    

    _Latency_Init(&requestLatency, "request");
    _Latency_Init(&executeAndReplyLatency, "executeAndReply");

    if (initialize) { //initialize == true
        if (AmLeader) {
            nullCommitTimeout->Start();
	    memset(src, 'v', 1);
	    rdma_client_send();//no need for ack from client side
	    process_work_completion_events(io_completion_channel, wc, 1);
        } else {
            viewChangeTimeout->Start();
	    //no need to send message to RDMA server since Amleader is hard-coded as true
        }
    } else {
        this->status = STATUS_RECOVERING;
        this->recoveryNonce = GenerateNonce();
        SendRecoveryMessages();
        recoveryTimeout->Start();
	//no need to send message since initialize == true
    }
}

		
//destructor should not need any change
VRReplica::~VRReplica()
{
    Latency_Dump(&requestLatency);
    Latency_Dump(&executeAndReplyLatency);

    //delete viewChangeTimeout;
    delete nullCommitTimeout;
    //delete stateTransferTimeout;
    delete resendPrepareTimeout;
    delete closeBatchTimeout;
    //delete recoveryTimeout;

    for (auto &kv : pendingPrepares) {
        delete kv.first;
    }
    client_disconnect_and_clean();
}
		  
uint64_t
VRReplica::GenerateNonce() const
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);
}
/*fast-path && non-leader
bool
VRReplica::AmLeader() const
{
    return (configuration.GetLeaderIndex(view) == this->replicaIdx);
}		  
*/
void
VRReplica::SendRecoveryMessages()
{
    ToReplicaMessage m;
    RecoveryMessage *recovery = m.mutable_recovery();
    recovery->set_replicaidx(this->replicaIdx);
    recovery->set_nonce(recoveryNonce);
    
    RNotice("Requesting recovery");
    
    
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send Recovery message to all replicas");
    }
    
}
//send prepare message
//No need tidy up
void
VRReplica::CloseBatch()
{
    struct ibv_wc wc[3];
    ASSERT(AmLeader);
    ASSERT(lastBatchEnd < lastOp);

    opnum_t batchStart = lastBatchEnd+1;
    memset(src, 'l', 1); //lowercase L for CloseBatch()
    memcpy(src+1, &batchstart, sizeof(batchstart));
    rdma_client_send();
    //need a client_receive() for case 'b' for CloseBatch--PBMessage(lastPrepare) from server;
    rdma_client_receive();//client_receive() case 'b'
    /*move this part logic to N10
    RDebug("Sending batched prepare from " FMT_OPNUM
           " to " FMT_OPNUM,
           batchStart, lastOp);
    /* Send prepare messages
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
    */
    /*	move to client_receive case 'b'
    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        RWarning("Failed to send prepare message to all replicas");
    }
    */
    lastBatchEnd = lastOp;
    batchComplete = false;
    resendPrepareTimeout->Reset();
    closeBatchTimeout->Stop();
    memset(src, 't', 1);
    memcpy(sr+1, &lastOp, sizeof(lastOp));
    rdma_client_send();
    process_work_completion_events(io_completion_channel, wc, 3); //an ack to gurantee receive
}
  
void
VRReplica::SendRecoveryMessages()
{
    ToReplicaMessage m;
    RecoveryMessage *recovery = m.mutable_recovery();
    recovery->set_replicaidx(this->replicaIdx);
    recovery->set_nonce(recoveryNonce);

    RNotice("Requesting recovery");
    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        RWarning("Failed to send Recovery message to all replicas");
    }
}
	
void
VRReplica::SendNullCommit()
{
    ToReplicaMessage m;
    CommitMessage *c = m.mutable_commit();
    c->set_view(this->view);
    c->set_opnum(this->lastCommitted);

    ASSERT(AmLeader());

    if (!(transport->SendMessageToAll(this, PBMessage(m)))) {
        RWarning("Failed to send null COMMIT message to all replicas");
    }
}

void
VRReplica::ResendPrepare()
{
    ASSERT(AmLeader);
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
        case ToReplicaMessage::MsgCase::kRequest:{
            HandleRequest(remote, replica_msg.request());
            break;
	}
	case ToReplicaMessage::MsgCase::kPrepareOk:{
            HandlePrepareOK(remote, replica_msg.prepare_ok());
	    break;
	}
	//all cases below should be executed on the Host
        case ToReplicaMessage::MsgCase::kUnloggedRequest:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleUnloggedRequest(remote, replica_msg.unlogged_request());
	    //send remote
	    memset(src, 'b', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    memcpy(src+1+sizeof(remote), replica_msg.unlogged_request(), sizeof(replica_msg.unlogged_request()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
        }
        case ToReplicaMessage::MsgCase::kPrepare:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandlePrepare(remote, replica_msg.prepare());
	    //send remote
	    memset(src, 'c', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //prepare
	    memcpy(src+1+sizeof(remote), replica_msg.prepare(), sizeof(replica_msg.prepare()));
	    rdma_client_send();
	    rdma_client_receive();
	    break;
        }
        case ToReplicaMessage::MsgCase::kCommit:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleCommit(remote, replica_msg.commit());
	    //send remote
	    memset(src, 'd', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //commit
	    memcpy(src+1+sizeof(remote), replica_msg.commit(), sizeof(replica_msg.commit()));
	    rdma_client_send();
	    //process_work_completion_events(io_completion_channel, wc, 1);
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kRequestStateTransfer:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleRequestStateTransfer(remote,replica_msg.request_state_transfer());
	    //send remote
	    memset(src, 'e', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //req state transfer
	    memcpy(src+1+sizeof(remote), replica_msg.request_state_transfer(), sizeof(replica_msg.request_state_transfer()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}	    //all lines below have not been scrutinized
        case ToReplicaMessage::MsgCase::kStateTransfer:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleStateTransfer(remote, replica_msg.state_transfer());
	    //send remote
	    memset(src, 'f', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //state transfer
	    memcpy(src+1+sizeof(remote), replica_msg.state_transfer(), sizeof(replica_msg.state_transfer()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kStartViewChange:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process. 
            //HandleStartViewChange(remote, replica_msg.start_view_change());
	    memset(src, 'g', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //start view change
	    memcpy(src+1+sizeof(remote), replica_msg.start_view_change(), sizeof(replica_msg.start_view_change()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kDoViewChange:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleDoViewChange(remote, replica_msg.do_view_change());
	    memset(src, 'h', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //do view change
	    memcpy(src+1+sizeof(remote), replica_msg.do_view_change(), sizeof(replica_msg.do_view_change()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kStartView:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleStartView(remote, replica_msg.start_view());
	    memset(src, 'i', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //Start view
	    memcpy(src+1+sizeof(remote), replica_msg.start_view(), sizeof(replica_msg.start_view()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kRecovery:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleRecovery(remote, replica_msg.recovery());
	    memset(src, 'j', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //recovery
	    memcpy(src+1+sizeof(remote), replica_msg.recovery(), sizeof(replica_msg.recovery()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        case ToReplicaMessage::MsgCase::kRecoveryResponse:{
            //this should be moved to Host. Let Host as RDMA client, do rdma read and process.
            //HandleRecoveryResponse(remote, replica_msg.recovery_response());
	    memset(src, 'k', 1);
	    memcpy(src+1, remote, sizeof(remote));
	    //recovery response
	    memcpy(src+1, replica_msg.recovery_response(), sizeof(replica_msg.recovery_response()));
	    rdma_client_send();
	    rdma_client_receive();
            break;
	}
        default:
            //the line below should not need further change
            RPanic("Received unexpected message type %u",replica_msg.msg_case());
    }
}

//this function might still change the state that N10 might need to know
void
VRReplica::HandleRequest(const TransportAddress &remote,
                         const RequestMessage &msg)
{
    struct ibv_wc wc;
    viewstamp_t v;
    Latency_Start(&requestLatency);

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status");
        Latency_EndType(&requestLatency, 'i');
        return;
    }

    if (!AmLeader) {
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
    memset(src, 'B', 1);
    int size = sizeof(clientAddresses);
    memcpy(src+1, &size, sizeof(int));
    memcpy(src+1+sizeof(int), &clientAddresses, sizeof(clientAddresses));

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
    
    //UpdateClientTable(msg.req()); on N10
    memcpy(src+1+sizeof(int)+sizeof(clientAddresses), &msg.req, sizeof(msg.req));
	
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
	memcpy(src+1+sizeof(clientAddresses)+sizeof(meg.req), &lastOp, sizeof(lastOp));
        v.view = this->view;
        v.opnum = this->lastOp;

        RDebug("Received REQUEST, assigning " FMT_VIEWSTAMP, VA_VIEWSTAMP(v));

        /* Add the request to my log */
	newlogentry = new LogEntry(v, LOG_STATE_PREPARED, request);
        log.Append(newlogentry);
        memcpy(src+1+sizeof(int)+sizeof(clientAddresses)+sizeof(meg.req)+sizeof(lastOp), &newlogentry, sizeof(LogEntry));
        if (batchComplete ||
            (lastOp - lastBatchEnd+1 > (unsigned int)batchSize)) {
            CloseBatch();
        } else {
            RDebug("Keeping in batch");
            if (!closeBatchTimeout->Active()) {
                closeBatchTimeout->Start();
		memset(src, 'C', 1);
		rdma_client_send();
		nullCommitTimeout->Reset();
        	Latency_End(&requestLatency);
		process_work_completion_events(io_completion_channel, &wc, 1);
		return;
            }
        }

        nullCommitTimeout->Reset();
        Latency_End(&requestLatency);
	rdma_client_send();
	process_work_completion_events(io_completion_channel, &wc, 1);
    }
}

//this function might still change the state that N10 might need to know
void
VRReplica::HandlePrepareOK(const TransportAddress &remote,
                           const PrepareOKMessage &msg)
{
    struct ibv_wc wc;
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
        //RequestStateTransfer();
	memset(src, 'D', 1);
	rdma_client_send();
	process_work_completion_events(io_completion_channel, &wc, 1);
        return;
    }

    if (!AmLeader) {
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
	memset(src, 'E', 1);
	memcpy(src+1, &vs, sizeof(vs));
	memcpy(src+1+sizeof(vs), &msg, sizeof(msg));
	

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
        rdma_client_send();
	process_work_completion_events(io_completion_channel, &wc, 1); 
        // XXX Adaptive batching -- make this configurable
        if (lastBatchEnd == msg.opnum()) {
            batchComplete = true;//batchcomplete seems to be not important to logic on N10, we dont send this bool for now
            if  (lastOp > lastBatchEnd) {
                CloseBatch();
            }
        }
    }
}

//orignially this function include 1st RDMA Write from client to server + 2nd RDMA Read from client to server (execute as ordered)
//I will divide the function into 2 separate function, RDMA Write & RDMA Read
//Actually there are 2 approaches, 1 is frequent write(to override), 1 is atomic operation. 
//But according to Page 106, Mellanox Verbs Programming Tutorial (by Dotan Barak) , atomic operation is performance killer
static int 
VRReplica::rdma_client_send()
{
	//struct ibv_wc wc;
	
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
	/*
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	debug("Client side SEND is complete \n");
	*/
	memset(src, 0, sizeof(src));
}

//this function is RDMA read: this function could only do Memory Region Level Read, can not do 
static int 
VRReplica::rdma_client_receive()
{       struct ibv_wc wc[2];
	memset(dst,0, sizeof(dst));
	memset(type, 0, sizeof(type));
	/* Now we prepare a READ using same variables but for destination */
	server_recv_sge.addr = (uint64_t) client_dst_mr->addr;
	server_recv_sge.length = (uint32_t) client_dst_mr->length;
	server_recv_sge.lkey = client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	/* Now we post it */
	ret = ibv_post_recv(client_qp, 
		       &server_recv_wr,
	       &bad_server_recv_wr);
	if (ret) {
		rdma_error("Failed to receive client dst buffer from the server, errno: %d \n", 
				-errno);
		return -errno;
	}
	// at this point we are expecting 1 work completion for the write 
	//leave process_work_completion_events()
	debug("Client side receive is complete \n");
	
	memcpy(type, dst, 1);
	switch(*type){
		case 'a':{//ack; for situation that same function may have different returns
		    process_work_completion_events(io_completion_channel, wc, 2);
		    break;
		}
		case 'b':{ //CloseBatch--PBMessage(lastPrepare)
		    process_work_completion_events(io_completion_channel, wc, 2);
		    memcpy(&lastPrepare, dst, sizeof(lastPrepare));
		    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        	    RWarning("Failed to send prepare message to all replicas");
   		    }
		    break;
		}
		case 'c':{//HandleUnlogged--ToClientMessage m
		    process_work_completion_events(io_completion_channel, wc, 2);
	    	    ToclientMessage m;
	   	    memcpy(&m, dst+1, sizeof(m));
	    	    //ExecuteUnlogged(msg.req(), *reply); not sure what it would do
	            if (!(transport->SendMessage(this, remote, PBMessage(m))))
           	    Warning("Failed to send reply message");
		    break;
		}
		case 'd':{//HandlePrepare--ToClientMessage m
		    process_work_completion_events(io_completion_channel, wc, 2);
	            int leader = (view % 3); //hard-coded n=3
	            ToReplicaMessage m;
                    memcpy(&m, dst+1, sizeof(m));
                    if (!(transport->SendMessageToReplica(this,leader,PBMessage(m)))) {
                    RWarning("Failed to send PrepareOK message to leader");
	            rdma_client_receive();
		    break;
		}
		case 'e':{//handleStateTransfer--new lastOp
		    process_work_completion_events(io_completion_channel, wc, 2);
	    	    memcpy(&lastOp, dst+1, sizeof(lastOp));
		    break;
		}
		case 'f':{//HandleStartViewChange--ToReplicaMessage m;
		    process_work_completion_events(io_completion_channel, wc, 2);
		    int leader = (view % 3); //hard-coded n=3
		    ToReplicaMessage m;
                    memcpy(&m, dst+1, sizeof(m));
		    if (!(transport->SendMessageToReplica(this, leader, PBMessage(m)))) {
                    RWarning("Failed to send DoViewChange message to leader of new view");
                    }
		    break;
		}
		case 'g':{//HandleDoViewChange--lastOp changed + ToReplicaMessage m
		    process_work_completion_events(io_completion_channel, wc, 2);
		    memcpy(&lastOp, dst+1, sizeof(lastOp));
		    ToReplicaMessage m;
                    memcpy(&m, dst+1+sizeof(lastOp), sizeof(m));
		    if (!(transport->SendMessageToAll(this, PBMessage(m)))) {
            	    RWarning("Failed to send StartView message to all replicas");
        	    }
		    break;
		}
		case 'h':{//HandleStartView--lastOp changed
		    process_work_completion_events(io_completion_channel, wc, 2);
		    memcpy(&lastOp, dst+1, sizeof(lastOp));
	            rdma_client_receive();
		    break;
		}
		case 'i':{//HandleRecovery--ToReplicaMessage m 
		    process_work_completion_events(io_completion_channel, wc, 2);
		    ToReplicaMessage m;
		    memcpy(&m, dst+1, sizeof(m));
		    if (!(transport->SendMessage(this, remote, PBMessage(m)))) {
                    RWarning("Failed to send recovery response");
                    }
		    break;
		}
		case 'j':{//HandleRecoveryResponse--lastOp changed
		    process_work_completion_events(io_completion_channel, wc, 2);
		    memcpy(&lastOp, dst+1, sizeof(lastOp));
		    rdma_client_receive();
		    break;
		}
		//below are reserved for non-handle functions()
		case 'k':{//CommitUpto--Latency_Start
		    process_work_completion_events(io_completion_channel, wc, 1);
		    Latency_Start(&executeAndReplyLatency);
		    rdma_client_receive();
		    break;
		}
		case 'l':{//Latency_End(&executeAndReplyLatency)-still in while loop, transport->SendMessage()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    Latency_End(&executeAndReplyLatency);
		    int n;
		    memcpy(&n, dst+1, sizeof(int));
		    ToClientMessage m;
		    memcpy(&m, dst+1+sizeof(int), sizeof(m));
		    auto iter = clientAddresses.find(n);
		    transport->SendMessage(this, *iter->second, PBMessage(m));
		    rdma_client_receive();
		    break;
		}
		/*
		case 'm':{//Latency_End(&executeAndReplyLatency)-while loop end, transport->SendMessage()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    int n;
		    memcpy(&n, dst+1, sizeof(int));
		    ToClientMessage m;
		    memcpy(&m, dst+1+sizeof(int), sizeof(m));
		    auto iter = clientAddresses.find(n);
		    transport->SendMessage(this, *iter->second, PBMessage(m)); 
		    break;
		}
		*/
		case 'n':{//Latency_End(&executeAndReplyLatency)-still in while loop, NO transport->SendMessage()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    rdma_client_receive();
		    break;
		}
		/*
		case 'o':{//Latency_End(&executeAndReplyLatency)-while loop end, NO transport->SendMessage()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    break;
		}
		*/
	        case 'p':{//Not Defined
		    process_work_completion_events(io_completion_channel, wc, 1);
		    break;
		}
		case 'q':{//sendPrepareOks->transport
		    process_work_completion_events(io_completion_channel, wc, 1);
		    ToReplicaMessage m;
		    int leader = (view % 3);
		    memcpy(&m, dst+1, sizeof(m));
		    if (!(transport->SendMessageToReplica(this,leader,PBMessage(m)))) {
                    RWarning("Failed to send PrepareOK message to leader");
                    }
		    break;
		}
		case 'r':{//Not defined
		    process_work_completion_events(io_completion_channel, wc, 1);
		    break;
		}
	        case 's':{//RequestStateTransfer()->transport
		    process_work_completion_events(io_completion_channel, wc, 1);
		    ToReplicaMessage m;
		    memcpy(&m, dst+1, sizeof(m));
		    if (!transport->SendMessageToAll(this, PBMessage(m))) {
        	    RWarning("Failed to send RequestStateTransfer message to all replicas");
   		    }
		    break;
		}
		case 't':{//EnterView->Amleader==true (view, stauts, lastBatched, 
			//batchcomplete, nullCommitTO->start()), prepareOKQuorum.Clear(); client_receive()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    nullCommitTimeout->Start();
		    Amleader = true;
		    status = STATUS_NORMAL;
		    memcpy(&view, dst+1, sizeof(view));
		    memcpy(&lastBatchEnd, dst+1+sizeof(view), sizeof(lastBatchEnd));
		    batchComplete = true;
		    prepareOKQuorum.Clear();
		    rdma_client_receive();
		    break;
		}
		case 'u':{//EnterView->Amleader==false (view, stauts, lastBatched, batchcomplete,
			//nullCommitTO->stop, resendPrepareTO->stop, closeBatchTO->stop()), 
			//prepareOKQuorum.Clear();, client_receive()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    nullCommitTimeout->Stop();
        	    resendPrepareTimeout->Stop();
        	    closeBatchTimeout->Stop();
		    Amleader = false;
		    status = STATUS_NORMAL;
		    memcpy(&view, dst+1, sizeof(view));
		    memcpy(&lastBatchEnd, dst+1+sizeof(view), sizeof(lastBatchEnd));
		    batchComplete = true;
		    prepareOKQuorum.Clear();
		    rdma_client_receive();
		    break;
		}
		case 'v':{//StartViewChange+view, status, nullCommitTimeout->Stop();
			//resendPrepareTimeout->Stop();closeBatchTimeout->Stop();client_receive()
		    process_work_completion_events(io_completion_channel, wc, 1);
		    nullCommitTimeout->Stop();
      	 	    resendPrepareTimeout->Stop();
    		    closeBatchTimeout->Stop();
		    status = STATUS_VIEW_CHANGE;
		    memcpy(&view, dst+1, sizeof(view));
		    rdma_client_receive();
		    break;
		}
		case 'w':{//StartViewChange->transport
		    process_work_completion_events(io_completion_channel, wc, 1);
		    ToReplicaMessage m;
		    memcpy(&m, dst+1, sizeof(m));
		    if (!transport->SendMessageToAll(this, PBMessage(m))) {
       		    RWarning("Failed to send StartViewChange message to all replicas");
    		    }
		    rdma_client_receive();
		    break;
		}
		case 'x':{//UpdateClientTable->clienttable
		    process_work_completion_events(io_completion_channel, wc, 1);
		    int sizeofclientTable;
		    memcpy(&sizeofclientTable, dst+1, sizeof(int));
		    memcpy(clientTable, &dst+1+sizeof(int), sizeofclientTable);
		    break;
		}
		case 'y':{//CloseBatch->transport
		    process_work_completion_events(io_completion_channel, wc, 1);
		    memcpy(&lastPrepare, dst+1, sizeof(lastPrepare));
		    if (!(transport->SendMessageToAll(this, PBMessage(lastPrepare)))) {
        	    RWarning("Failed to send prepare message to all replicas");
                    }
		    break;
		}
		case 'z':{//HanldeRequestStateTransfer()->transport
		    process_work_completion_events(io_completion_channel, wc, 1);
		    ToReplicaMessage m;
		    memcpy(&m, dst+1, sizeof(m));
		    transport->SendMessage(this, remote, PBMessage(m));
		    break;
		}
	}
	return 0;
} 

		  
/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
}
}
