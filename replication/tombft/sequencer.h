#pragma once

#include "lib/message.h"
#include "lib/signature.h"
#include "replication/tombft/message.h"
#include "sequencer/sequencer.h"

namespace dsnet {
namespace tombft {

class TomBFTSequencer : public Sequencer {
 public:
  TomBFTSequencer(const Configuration &config, Transport *transport,
                  const Security &s, int id)
      : Sequencer(config, transport, id),
        s(s),
        id(id),
        sess_num(id + 1),
        msg_num(0) {}
  ~TomBFTSequencer() {}

  virtual void ReceiveMessage(const TransportAddress &remote, void *buf,
                              size_t size) override {
    auto header = reinterpret_cast<TomBFTMessage::Header *>(buf);
    Assert(header->sess_num == 0);
    Assert(size >= sizeof(TomBFTMessage::Header));
    msg_num += 1;
    header->sess_num = HTON_SESSNUM(sess_num);
    header->msg_num = HTON_MSGNUM(msg_num);

    std::string msg;
    msg.assign(reinterpret_cast<char *>(buf) + sizeof(TomBFTMessage::Header),
               size - sizeof(TomBFTMessage::Header));
    for (int i = 0; i < 4; i += 1) {  // TODO configurable replica number
      std::string replica_sig;
      s.SequencerSigner(i, id).Sign(msg, replica_sig);
      replica_sig.copy(header->hmac_list[i], SHA256_DIGEST_LENGTH);
    }
    transport_->SendMessageToAll(this, BufferMessage(buf, size));
  }

 private:
  const Security &s;
  int id;
  const uint16_t sess_num;
  uint64_t msg_num;
};

}  // namespace tombft
}  // namespace dsnet
