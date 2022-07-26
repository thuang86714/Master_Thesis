#pragma once

#include <map>

#include "lib/transport.h"

namespace dsnet {

typedef uint16_t SessNum;
typedef uint64_t MsgNum;

class BufferMessage : public Message {
public:
    BufferMessage(const void *buf, size_t size);
    ~BufferMessage();

    virtual BufferMessage *Clone() const override;
    virtual std::string Type() const override;
    virtual size_t SerializedSize() const override;
    virtual void Parse(const void *buf, size_t size) override;
    virtual void Serialize(void *buf) const override;

    const void *GetBuffer() const;
    size_t GetBufferSize() const;

private:
    const void *buf_;
    size_t size_;
};

class Sequencer : public TransportReceiver {
public:
    Sequencer(const Configuration &config, Transport *transport, int id);
    virtual ~Sequencer();

protected:
    const Configuration &config_;
    Transport *transport_;
};

} // namespace dsnet
