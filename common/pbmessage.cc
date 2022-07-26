#include "common/pbmessage.h"

namespace dsnet {

PBMessage::PBMessage(::google::protobuf::Message &msg)
    : msg_(&msg) { }

PBMessage::~PBMessage() { }

PBMessage *
PBMessage::Clone() const
{
    return new PBMessage(*msg_);
}

std::string
PBMessage::Type() const
{
    return msg_->GetTypeName();
}

size_t
PBMessage::SerializedSize() const
{
    return msg_->ByteSizeLong();
}

void
PBMessage::Parse(const void *buf, size_t size)
{
    msg_->ParseFromArray(buf, size);
}

void
PBMessage::Serialize(void *buf) const
{
    msg_->SerializeToArray(buf, SerializedSize());
}

} // namespace dsnet
