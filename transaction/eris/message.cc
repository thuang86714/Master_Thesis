#include "lib/message.h"
#include "transaction/eris/eris-proto.pb.h"
#include "transaction/eris/message.h"

namespace dsnet {
namespace transaction {
namespace eris {

/*
 * Packet format:
 * sequencer header size  + sess num + number of groups + each (group id + msg num)
 */

size_t
Multistamp::SerializedSize() const
{
    return sizeof(SessNum) + sizeof(NumGroups) +
        msg_nums.size() * (sizeof(GroupID) + sizeof(MsgNum));
}

ErisMessage::ErisMessage(::google::protobuf::Message &msg)
    : PBMessage(msg) { }

ErisMessage::ErisMessage(::google::protobuf::Message &msg, const std::vector<int> &groups)
    : PBMessage(msg)
{
    for (const int group : groups) {
        stamp_.msg_nums[group] = 0;
    }
}

ErisMessage::~ErisMessage() { }

ErisMessage *
ErisMessage::Clone() const
{
    ErisMessage *m = new ErisMessage(*msg_);
    m->stamp_ = stamp_;
    return m;
}

size_t
ErisMessage::SerializedSize() const
{
    size_t sz = sizeof(HeaderSize);
    if (stamp_.msg_nums.size() > 0) {
        sz += stamp_.SerializedSize();
    }
    return sz + PBMessage::SerializedSize();
}

void
ErisMessage::Parse(const void *buf, size_t size)
{
    const char *p = (const char*)buf;
    HeaderSize header_sz = NTOH_HEADERSIZE(*(HeaderSize *)p);
    p += sizeof(HeaderSize);
    if (header_sz > 0) {
        stamp_.sess_num = NTOH_SESSNUM(*(SessNum *)p);
        p += sizeof(SessNum);
        NumGroups n_groups = NTOH_NUMGROUPS(*(NumGroups *)p);
        p += sizeof(NumGroups);
        for (int i = 0; i < n_groups; i++) {
            GroupID id = NTOH_GROUPID(*(GroupID *)p);
            p += sizeof(GroupID);
            MsgNum msg_num = NTOH_MSGNUM(*(MsgNum *)p);
            p += sizeof(MsgNum);
            stamp_.msg_nums[id] = msg_num;
        }
    }
    PBMessage::Parse(p, size - sizeof(HeaderSize) - header_sz);
    if (header_sz > 0) {
        // Only client request message will be tagged with multistamp
        proto::ToServerMessage &to_server =
            dynamic_cast<proto::ToServerMessage &>(*msg_);
        if (to_server.msg_case() !=
                proto::ToServerMessage::MsgCase::kRequest) {
            Panic("Received multistamp with wrong message type");
        }
        dsnet::Request *request = to_server.mutable_request()->mutable_request();
        request->set_sessnum(stamp_.sess_num);
        for (auto it = request->mutable_ops()->begin();
                it != request->mutable_ops()->end();
                it++) {
            it->set_msgnum(stamp_.msg_nums.at(it->shard()));
        }
    }
}

void
ErisMessage::Serialize(void *buf) const
{
    char *p = (char *)buf;
    *(HeaderSize *)p = HTON_HEADERSIZE(stamp_.msg_nums.size() > 0 ?
            stamp_.SerializedSize() : 0);
    p += sizeof(HeaderSize);
    if (stamp_.msg_nums.size() > 0) {
        // sess num filled by sequencer
        p += sizeof(SessNum);
        *(NumGroups *)p = HTON_NUMGROUPS(stamp_.msg_nums.size());
        p += sizeof(NumGroups);
        for (const auto &kv : stamp_.msg_nums) {
            *(GroupID *)p = HTON_GROUPID(kv.first);
            p += sizeof(GroupID);
            // msg num filled by sequencer
            p += sizeof(MsgNum);
        }
    }
    PBMessage::Serialize(p);
}

const Multistamp &
ErisMessage::GetStamp() const
{
    return stamp_;
}

} // namespace eris
} // namespace transaction
} // namespace dsnet
