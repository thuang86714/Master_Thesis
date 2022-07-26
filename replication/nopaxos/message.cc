#include "lib/message.h"
#include "replication/nopaxos/nopaxos-proto.pb.h"
#include "replication/nopaxos/message.h"

namespace dsnet {
namespace nopaxos {

using namespace proto;

/*
 * Packet format:
 * sequencer header size + sess num + msg num
 */

NOPaxosMessage::NOPaxosMessage(::google::protobuf::Message &msg, bool sequencing)
    : PBMessage(msg), sequencing_(sequencing) { }

NOPaxosMessage::~NOPaxosMessage() { }

NOPaxosMessage *
NOPaxosMessage::Clone() const
{
    return new NOPaxosMessage(*msg_, sequencing_);
}

size_t
NOPaxosMessage::SerializedSize() const
{
    size_t sz = sizeof(HeaderSize);
    if (sequencing_) {
        sz += sizeof(SessNum) + sizeof(MsgNum);
    }
    return sz + PBMessage::SerializedSize();
}

void
NOPaxosMessage::Parse(const void *buf, size_t size)
{
    SessNum sess_num;
    MsgNum msg_num;
    const char *p = (const char*)buf;
    HeaderSize header_sz = NTOH_HEADERSIZE(*(HeaderSize *)p);
    p += sizeof(HeaderSize);
    if (header_sz > 0) {
        sess_num = NTOH_SESSNUM(*(SessNum *)p);
        p += sizeof(SessNum);
        msg_num = NTOH_MSGNUM(*(MsgNum *)p);
        p += sizeof(MsgNum);
    }
    PBMessage::Parse(p, size - sizeof(HeaderSize) - header_sz);
    if (header_sz > 0) {
        // Only client request message will be tagged with stamp
        ToReplicaMessage &to_replica =
            dynamic_cast<proto::ToReplicaMessage &>(*msg_);
        if (to_replica.msg_case() !=
                proto::ToReplicaMessage::MsgCase::kRequest) {
            Panic("Received stamp with wrong message type");
        }
        proto::RequestMessage *request = to_replica.mutable_request();
        request->set_sessnum(sess_num);
        request->set_msgnum(msg_num);
    }
}

void
NOPaxosMessage::Serialize(void *buf) const
{
    char *p = (char *)buf;
    *(HeaderSize *)p = HTON_HEADERSIZE(sequencing_ ? sizeof(SessNum) + sizeof(MsgNum) : 0);
    p += sizeof(HeaderSize);
    if (sequencing_) {
        // sess num filled by sequencer
        p += sizeof(SessNum);
        // msg num filled by sequencer
        p += sizeof(MsgNum);
    }
    PBMessage::Serialize(p);
}

} // namespace nopaxos
} // namespace dsnet
