#include "replication/tombft/client.h"

#include "replication/tombft/message.h"

namespace dsnet {
namespace tombft {

TomBFTClient::TomBFTClient(const Configuration &config,
                           const ReplicaAddress &addr, Transport *transport,
                           const Security &security, uint64_t client_id)
    : Client(config, addr, transport, client_id),
      security(security),
      pending_request(nullptr),
      reply_set(config.f * 2 + 1) {
  last_req_id = 0;

  request_timeout = new Timeout(transport, 1000, []() { NOT_IMPLEMENTED(); });
}

TomBFTClient::~TomBFTClient() {
  delete request_timeout;
  if (pending_request) {
    delete pending_request;
  }
}

void TomBFTClient::InvokeUnlogged(int replicaIdx, const string &request,
                                  continuation_t continuation,
                                  timeout_continuation_t timeoutContinuation,
                                  uint32_t timeout) {
  NOT_IMPLEMENTED();
}

void TomBFTClient::Invoke(const string &request, continuation_t continuation) {
  if (pending_request) {
    Panic("duplicated pending request");
  }
  last_req_id += 1;
  pending_request = new PendingRequest(request, last_req_id, continuation);
  SendRequest();
}

void TomBFTClient::SendRequest() {
  proto::Message msg;
  msg.mutable_request()->mutable_req()->set_op(pending_request->request);
  msg.mutable_request()->mutable_req()->set_clientid(clientid);
  msg.mutable_request()->mutable_req()->set_clientreqid(
      pending_request->client_req_id);
  msg.mutable_request()->mutable_req()->set_clientaddr(node_addr_->Serialize());
  security.ClientSigner().Sign(msg.request().SerializeAsString(),
                               *msg.mutable_request()->mutable_sig());
  if (config.NumSequencers()) {
    transport->SendMessageToSequencer(this, 0, TomBFTMessage(msg, true));
  } else {
    transport->SendMessageToMulticast(this, TomBFTMessage(msg, true));
  }
  request_timeout->Start();
}

void TomBFTClient::ReceiveMessage(const TransportAddress &remote, void *buf,
                                  size_t size) {
  proto::Message msg;
  TomBFTMessage m(msg);
  m.Parse(buf, size);

  switch (msg.msg_case()) {
    case proto::Message::kReply:
      HandleReply(remote, msg.reply());
      break;
    default:
      Panic("unexpected message type: %d", msg.msg_case());
  }
}

void TomBFTClient::HandleReply(const TransportAddress &remote,
                               const proto::ReplyMessage &msg) {
  if (!pending_request) {
    return;
  }
  if (msg.clientreqid() != pending_request->client_req_id) {
    return;
  }
  proto::ReplyMessage cloned(msg);
  cloned.set_sig(string());
  if (!security.ReplicaVerifier(msg.replicaid())
           .Verify(cloned.SerializeAsString(), msg.sig())) {
    Warning("signature verification failed");
    return;
  }
  cloned.set_replicaid(0);
  if (!reply_set.Add(pending_request->client_req_id, msg.replicaid(), cloned)) {
    return;
  }
  Debug("received 2f + 1 matched replies, pending request done");
  continuation_t cont = pending_request->continuation;
  string req = pending_request->request;
  delete pending_request;
  pending_request = nullptr;
  cont(req, msg.reply());
}

}  // namespace tombft
}  // namespace dsnet