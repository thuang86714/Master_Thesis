//
#pragma once

#include "common/log.h"
#include "common/replica.h"
#include "lib/signature.h"
#include "replication/tombft/message.h"
#include "replication/tombft/tombft-proto.pb.h"

namespace dsnet {
namespace tombft {

struct TomBFTLogEntry : public LogEntry {
  std::unique_ptr<TomBFTMessage> msg;

  TomBFTLogEntry(viewstamp_t vs, LogEntryState state, const Request &req,
                 const TomBFTMessage &msg)
      : LogEntry(vs, state, req), msg(msg.Clone()) {}
};

class TomBFTReplica : public Replica {
 public:
  TomBFTReplica(const Configuration &config, int myIdx, bool initialize,
                Transport *transport, const Security &securtiy,
                AppReplica *app);
  ~TomBFTReplica() {}

  void ReceiveMessage(const TransportAddress &remote, void *buf,
                      size_t size) override;

 private:
  void HandleRequest(const proto::RequestMessage &msg,
                     const TransportAddress &remote,
                     const TomBFTMessage::Header &meta, const TomBFTMessage &m);

  const Security &security;
  viewstamp_t vs;
  Log log;
};

}  // namespace tombft
}  // namespace dsnet
