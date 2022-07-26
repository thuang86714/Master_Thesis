#pragma once

#include <unordered_map>
#include <vector>

#include "common/pbmessage.h"
#include "transaction/eris/types.h"

namespace dsnet {
namespace transaction {
namespace eris {

struct Multistamp {
    SessNum sess_num;
    std::unordered_map<GroupID, MsgNum> msg_nums;

    size_t SerializedSize() const;
};

class ErisMessage : public PBMessage
{
public:
    ErisMessage(::google::protobuf::Message &msg);
    ErisMessage(::google::protobuf::Message &msg, const std::vector<int> &groups);
    ~ErisMessage();

    virtual ErisMessage *Clone() const override;
    virtual size_t SerializedSize() const override;
    virtual void Parse(const void *buf, size_t size) override;
    virtual void Serialize(void *buf) const override;

    const Multistamp &GetStamp() const;

private:
    Multistamp stamp_;
};

} // namespace eris
} // namespace transaction
} // namespace dsnet
