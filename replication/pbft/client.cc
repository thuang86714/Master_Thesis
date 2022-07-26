#include "replication/pbft/client.h"

#include "common/client.h"
#include "common/pbmessage.h"
#include "common/request.pb.h"
#include "lib/message.h"
#include "lib/signature.h"
#include "lib/transport.h"
#include "replication/pbft/pbft-proto.pb.h"

namespace dsnet {
namespace pbft {

using namespace proto;

PbftClient::PbftClient(const Configuration &config, const ReplicaAddress &addr,
                       Transport *transport, const Security &sec,
                       uint64_t clientid)
    : Client(config, addr, transport, clientid), security(sec) {
  lastReqId = 0;
  pendingRequest = nullptr;
  // requestTimeout = new Timeout(transport, 1000, [this]() { ResendRequest(); });
  // workaround for bench
  requestTimeout = new Timeout(transport, 10, [this]() { ResendRequest(); });

  view = 0;
}

PbftClient::~PbftClient() {
  delete requestTimeout;
  if (pendingRequest) {
    delete pendingRequest;
  }
}

void PbftClient::Invoke(const string &request, continuation_t continuation) {
  if (pendingRequest != NULL) {
    Panic("Client only supports one pending request");
  }
  lastReqId += 1;
  pendingRequest = new PendingRequest(request, lastReqId, continuation, config);
  SendRequest();
}

void PbftClient::SendRequest(bool broadcast) {
  ToReplicaMessage m;
  proto::RequestMessage &reqMsg = *m.mutable_request();
  reqMsg.mutable_req()->set_op(pendingRequest->request);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(lastReqId);

  security.ClientSigner().Sign(reqMsg.req().SerializeAsString(),
                               *reqMsg.mutable_sig());
  reqMsg.set_relayed(false);

  if (broadcast)
    transport->SendMessageToAll(this, PBMessage(m));
  else
    transport->SendMessageToReplica(this, config.GetLeaderIndex(view),
                                    PBMessage(m));
  requestTimeout->Reset();
}

void PbftClient::ResendRequest() {
  Warning("Timeout, resending request for req id %lu", lastReqId);
  SendRequest(true);
}

void PbftClient::InvokeUnlogged(int replicaIdx, const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation,
                                uint32_t timeout) {
  NOT_IMPLEMENTED();
}

void PbftClient::ReceiveMessage(const TransportAddress &remote, void *buf,
                                size_t size) {
  static ToClientMessage client_msg;
  static PBMessage m(client_msg);

  m.Parse(buf, size);

  switch (client_msg.msg_case()) {
    case ToClientMessage::MsgCase::kReply:
      HandleReply(remote, client_msg.reply());
      break;
    // TODO unlogged
    default:
      Panic("Received unexpected message type %u", client_msg.msg_case());
  }
}

void PbftClient::HandleReply(const TransportAddress &remote,
                             const proto::ReplyMessage &msg) {
  if (!pendingRequest) {
    // Warning("Received reply when no request was pending");
    return;
  }

  proto::ReplyMessage copy(msg);
  copy.set_sig(std::string());
  if (!security.ReplicaVerifier(msg.replicaid())
           .Verify(copy.SerializeAsString(), msg.sig())) {
    Warning("Received wrong signature");
    return;
  }

  if (msg.req().clientreqid() != pendingRequest->clientreqid) {
    return;
  }

  Debug("Client received reply, spec = %d", msg.speculative());
  if (!msg.speculative()) {
    if (!pendingRequest->replySet.Add(msg.req().clientreqid(), msg.replicaid(),
                                      msg.reply())) {
      return;
    } else {
      Debug("f + 1 replies received, done req = %lu",
            pendingRequest->clientreqid);
    }
  } else {
    if (!pendingRequest->specReplySet.Add(msg.req().clientreqid(),
                                          msg.replicaid(), msg.reply())) {
      return;
    } else {
      Debug("2f + 1 speculative replies received, done req = %lu",
            pendingRequest->clientreqid);
    }
  }

  requestTimeout->Stop();
  PendingRequest *req = pendingRequest;
  pendingRequest = nullptr;
  req->continuation(req->request, msg.reply());
  delete req;

  Assert(msg.view() >= view);
  view = msg.view();
}

}  // namespace pbft
}  // namespace dsnet
