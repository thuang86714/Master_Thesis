#include <vector>

#include "transaction/eris/sequencer.h"

namespace dsnet {
namespace transaction {
namespace eris {

ErisSequencer::ErisSequencer(const Configuration &config,
                             Transport *transport, int id)
    : Sequencer(config, transport, id),
      sess_num_(id) { }

ErisSequencer::~ErisSequencer() { }

void
ErisSequencer::ReceiveMessage(const TransportAddress &remote, void *buf, size_t size)
{
    std::vector<int> groups;
    char *p = (char *)buf;
    HeaderSize header_sz = NTOH_HEADERSIZE(*(HeaderSize *)p);
    p += sizeof(HeaderSize);
    if (header_sz > 0) {
        // Session number
        *(SessNum *)p = HTON_SESSNUM(sess_num_);
        p += sizeof(SessNum);
        // Message number for each group
        NumGroups n = NTOH_NUMGROUPS(*(NumGroups *)p);
        p += sizeof(NumGroups);
        for (int i = 0; i < n; i++) {
            GroupID g = NTOH_GROUPID(*(GroupID *)p);
            groups.push_back(g);
            p += sizeof(GroupID);
            *(MsgNum *)p = HTON_MSGNUM(++msg_nums_[g]);
            p += sizeof(MsgNum);
        }

        transport_->SendMessageToGroups(this, groups, BufferMessage(buf, size));
    }
}

} // namespace eris
} // namespace transaction
} // namespace dsnet
