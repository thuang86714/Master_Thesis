#include <cstring>
#include "lib/message.h"
#include "sequencer/sequencer.h"

namespace dsnet {

BufferMessage::BufferMessage(const void *buf, size_t size)
    : buf_(buf), size_(size) { }

BufferMessage::~BufferMessage() { }

BufferMessage *
BufferMessage::Clone() const
{
    return new BufferMessage(buf_, size_);
}

std::string
BufferMessage::Type() const
{
    return std::string("Buffer Message");
}

size_t
BufferMessage::SerializedSize() const
{
    return size_;
}

void
BufferMessage::Parse(const void *buf, size_t size) { }


void
BufferMessage::Serialize(void *buf) const
{
    memcpy(buf, buf_, size_);
}

const void *
BufferMessage::GetBuffer() const
{
    return buf_;
}

size_t
BufferMessage::GetBufferSize() const
{
    return size_;
}

Sequencer::Sequencer(const Configuration &config, Transport *transport, int id)
    : config_(config), transport_(transport)
{
    if (config.NumSequencers() <= id) {
        Panic("Address for sequencer %d not properly configured", id);
    }
    transport_->RegisterAddress(this, config, &config.sequencer(id));
}

Sequencer::~Sequencer() { }

} // namespace dsnet
