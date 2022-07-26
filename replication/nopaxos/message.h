#pragma once

#include "common/pbmessage.h"
#include "replication/nopaxos/types.h"

namespace dsnet {
namespace nopaxos {

class NOPaxosMessage : public PBMessage
{
public:
    NOPaxosMessage(::google::protobuf::Message &msg, bool sequencing = false);
    ~NOPaxosMessage();

    virtual NOPaxosMessage *Clone() const override;
    virtual size_t SerializedSize() const override;
    virtual void Parse(const void *buf, size_t size) override;
    virtual void Serialize(void *buf) const override;

private:
    bool sequencing_;
};

} // namespace nopaxos
} // namespace dsnet
