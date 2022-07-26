#pragma once

#include "common/client.h"
#include "common/quorumset.h"
#include "lib/configuration.h"
#include "lib/signature.h"
#include "replication/tombft/tombft-proto.pb.h"

namespace dsnet {
namespace tombft {

class TomBFTClient : public Client {
 public:
  TomBFTClient(const Configuration &config, const ReplicaAddress &addr,
               Transport *transport, const Security &security,
               uint64_t clientid = 0);
  virtual ~TomBFTClient();
  virtual void Invoke(const string &request,
                      continuation_t continuation) override;
  virtual void InvokeUnlogged(
      int replicaIdx, const string &request, continuation_t continuation,
      timeout_continuation_t timeoutContinuation = nullptr,
      uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
  virtual void ReceiveMessage(const TransportAddress &remote, void *buf,
                              size_t size) override;

 protected:
  const Security &security;
  uint64_t last_req_id;

  struct PendingRequest {
    string request;
    uint64_t client_req_id;
    continuation_t continuation;
    inline PendingRequest(string request, uint64_t clientReqId,
                          continuation_t continuation)
        : request(request),
          client_req_id(clientReqId),
          continuation(continuation) {}
  };
  PendingRequest *pending_request;
  Timeout *request_timeout;
  ByzantineProtoQuorumSet<uint64_t, proto::ReplyMessage> reply_set;

  void SendRequest();
  void HandleReply(const TransportAddress &remote,
                   const proto::ReplyMessage &msg);
};

}  // namespace tombft
}  // namespace dsnet
