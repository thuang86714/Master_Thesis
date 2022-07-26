#pragma once

#include <unordered_map>

#include "sequencer/sequencer.h"
#include "transaction/eris/types.h"

namespace dsnet {
namespace transaction {
namespace eris {

class ErisSequencer : public Sequencer {
public:
    ErisSequencer(const Configuration &config, Transport *transport, int id);
    ~ErisSequencer();

    virtual void ReceiveMessage(const TransportAddress &remote,
                                void *buf, size_t size) override;

private:
    SessNum sess_num_;
    std::unordered_map<GroupID, MsgNum> msg_nums_;
};

} // namespace eris
} // namespace transaction
} // dsnet
