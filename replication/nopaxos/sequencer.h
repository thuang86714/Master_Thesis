#pragma once

#include "sequencer/sequencer.h"
#include "replication/nopaxos/types.h"

namespace dsnet {
namespace nopaxos {

class NOPaxosSequencer : public Sequencer {
public:
    NOPaxosSequencer(const Configuration &config, Transport *transport, int id);
    ~NOPaxosSequencer();

    virtual void ReceiveMessage(const TransportAddress &remote,
                                void *buf, size_t size) override;

private:
    SessNum sess_num_;
    MsgNum msg_num_;
};

} // namespace nopaxos
} // namespace dsnet
