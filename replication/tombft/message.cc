//

#include "replication/tombft/message.h"

#include <cstring>

#include "lib/assert.h"

namespace dsnet {
namespace tombft {

void TomBFTMessage::Parse(const void *buf, size_t size) {
  auto bytes = reinterpret_cast<const uint8_t *>(buf);
  if (NTOH_SESSNUM(reinterpret_cast<const Header *>(buf)->sess_num != 0)) {
    const size_t header_size = sizeof(Header);
    Assert(size > header_size);
    std::memcpy(&meta, buf, header_size);
    meta.sess_num = NTOH_SESSNUM(meta.sess_num);
    meta.msg_num = NTOH_MSGNUM(meta.msg_num);
    bytes += sizeof(Header);
    size -= sizeof(Header);
  } else {
    meta.sess_num = 0;
    bytes += sizeof(size_t);
    size -= sizeof(size_t);
  }

  pb_msg.Parse(bytes, size);
}

void TomBFTMessage::Serialize(void *buf) const {
  auto bytes = reinterpret_cast<uint8_t *>(buf);
  size_t header_size;
  if (sequencing) {
    Header hton_meta(meta);
    hton_meta.sess_num = HTON_SESSNUM(hton_meta.sess_num);
    hton_meta.msg_num = HTON_MSGNUM(hton_meta.msg_num);
    header_size = sizeof(Header);
    std::memcpy(bytes, &hton_meta, header_size);
  } else {
    header_size = sizeof(size_t);
    reinterpret_cast<Header *>(buf)->sess_num = 0;
  }
  pb_msg.Serialize(bytes + header_size);
}

}  // namespace tombft
}  // namespace dsnet