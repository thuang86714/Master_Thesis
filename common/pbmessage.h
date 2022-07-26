#pragma once

#include <google/protobuf/message.h>

#include "lib/transport.h"

namespace dsnet {

class PBMessage : public Message
{
public:
    PBMessage(::google::protobuf::Message &msg);
    ~PBMessage();

    virtual PBMessage *Clone() const override;
    virtual std::string Type() const override;
    virtual size_t SerializedSize() const override;
    virtual void Parse(const void *buf, size_t size) override;
    virtual void Serialize(void *buf) const override;

    ::google::protobuf::Message &Message() { return *msg_; };

protected:
    ::google::protobuf::Message *msg_;
};

} // namespace dsnet
