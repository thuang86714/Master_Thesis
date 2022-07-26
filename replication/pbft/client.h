#ifndef _PBFT_CLIENT_H_
#define _PBFT_CLIENT_H_

#include <map>
#include <set>
#include <string>

#include "common/client.h"
#include "common/quorumset.h"
#include "lib/configuration.h"
#include "lib/signature.h"
#include "replication/pbft/pbft-proto.pb.h"

namespace dsnet {
namespace pbft {

class PbftClient : public Client {
 public:
  PbftClient(const Configuration &config, const ReplicaAddress &addr,
             Transport *transport, const Security &sec, uint64_t clientid = 0);
  virtual ~PbftClient();
  virtual void Invoke(const string &request,
                      continuation_t continuation) override;
  virtual void InvokeUnlogged(
      int replicaIdx, const string &request, continuation_t continuation,
      timeout_continuation_t timeoutContinuation = nullptr,
      uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
  virtual void ReceiveMessage(const TransportAddress &remote, void *buf,
                              size_t size) override;

 private:
  const Security &security;

  struct PendingRequest {
    std::string request;
    std::uint64_t clientreqid;
    continuation_t continuation;
    ByzantineQuorumSet<std::uint64_t, std::string> replySet;
    ByzantineQuorumSet<std::uint64_t, std::string> specReplySet;
    PendingRequest(string request, uint64_t clientreqid,
                   continuation_t continuation, const Configuration &config)
        : request(request),
          clientreqid(clientreqid),
          continuation(continuation),
          replySet(config.f + 1),
          specReplySet(2 * config.f + 1) {}
  };

  uint64_t lastReqId;
  PendingRequest *pendingRequest;
  Timeout *requestTimeout;
  view_t view;

  void HandleReply(const TransportAddress &remote,
                   const proto::ReplyMessage &msg);
  // void HandleUnloggedReply(const TransportAddress &remote,
  //                          const proto::UnloggedReplyMessage &msg);

  void SendRequest(bool broadcast = false);
  void ResendRequest();
};

}  // namespace pbft
}  // namespace dsnet

#endif /* _PBFT_CLIENT_H_ */
