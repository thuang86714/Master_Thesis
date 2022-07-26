#pragma once

#include <endian.h>
#include <arpa/inet.h>

namespace dsnet {
namespace transaction {
namespace eris {

typedef uint16_t SessNum;
#define HTON_SESSNUM(n) htons(n)
#define NTOH_SESSNUM(n) ntohs(n)
typedef uint32_t GroupID;
#define HTON_GROUPID(n) htonl(n)
#define NTOH_GROUPID(n) ntohl(n)
typedef uint64_t MsgNum;
#define HTON_MSGNUM(n) htobe64(n)
#define NTOH_MSGNUM(n) be64toh(n)
typedef uint16_t HeaderSize;
#define HTON_HEADERSIZE(n) htons(n)
#define NTOH_HEADERSIZE(n) ntohs(n)
typedef uint8_t NumGroups;
#define HTON_NUMGROUPS(n) n
#define NTOH_NUMGROUPS(n) n

} // namespace eris
} // namespace transaction
} // namespace dsnet
