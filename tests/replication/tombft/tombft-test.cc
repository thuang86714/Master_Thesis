#include <gtest/gtest.h>

#include "lib/signature.h"
#include "lib/simtransport.h"
#include "replication/tombft/client.h"
#include "replication/tombft/message.h"
#include "replication/tombft/replica.h"
#include "replication/tombft/sequencer.h"
#include "replication/tombft/tombft-proto.pb.h"

using namespace std;
using namespace dsnet;
using namespace dsnet::tombft;

TEST(TomBFT, MessageSerDe) {
  NopSecurity s;

  proto::Message msg;
  TomBFTMessage m(msg, true);
  dsnet::Request req;
  req.set_clientid(42);
  req.set_clientreqid(1);
  req.set_op("test op");
  *msg.mutable_request()->mutable_req() = req;
  s.ClientSigner().Sign(req.SerializeAsString(),
                        *msg.mutable_request()->mutable_sig());
  m.meta.sess_num = 1;
  m.meta.msg_num = 73;
  const size_t m_size = m.SerializedSize();
  ASSERT_LT(m_size, 1500);

  uint8_t *msg_buf = new uint8_t[m_size];
  m.Serialize(msg_buf);

  proto::Message parsed_msg;
  TomBFTMessage parsed_m(parsed_msg);
  parsed_m.Parse(msg_buf, m_size);
  ASSERT_EQ(parsed_m.meta.sess_num, m.meta.sess_num);
  ASSERT_EQ(parsed_m.meta.msg_num, m.meta.msg_num);
  ASSERT_TRUE(
      s.ClientVerifier().Verify(parsed_msg.request().req().SerializeAsString(),
                                parsed_msg.request().sig()));
  ASSERT_EQ(parsed_msg.request().req().clientid(),
            msg.request().req().clientid());
  ASSERT_EQ(parsed_msg.request().req().clientreqid(),
            msg.request().req().clientreqid());
  ASSERT_EQ(parsed_msg.request().req().op(), msg.request().req().op());

  // non-sequencing
  TomBFTMessage noseq_m(msg);
  ASSERT_LT(noseq_m.SerializedSize(), m_size);
  noseq_m.Serialize(msg_buf);

  parsed_m.Parse(msg_buf, noseq_m.SerializedSize());
  ASSERT_EQ(parsed_m.meta.sess_num, 0);
  ASSERT_TRUE(
      s.ClientVerifier().Verify(parsed_msg.request().req().SerializeAsString(),
                                parsed_msg.request().sig()));
  ASSERT_EQ(parsed_msg.request().req().clientid(),
            msg.request().req().clientid());
  ASSERT_EQ(parsed_msg.request().req().clientreqid(),
            msg.request().req().clientreqid());
  ASSERT_EQ(parsed_msg.request().req().op(), msg.request().req().op());

  delete msg_buf;
}

class TestApp : public AppReplica {
 public:
  vector<string> op_vec;
  void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                     void *arg = nullptr, void *ret = nullptr) override {
    op_vec.push_back(req);
    reply = "reply: " + req;
  }
};

TEST(TomBFT, 1Op) {
  map<int, vector<ReplicaAddress> > replicaAddrs = {{0,
                                                     {{"localhost", "1509"},
                                                      {"localhost", "1510"},
                                                      {"localhost", "1511"},
                                                      {"localhost", "1512"}}}};
  Configuration c(1, 4, 1, replicaAddrs, nullptr, {{"localhost", "8888"}});
  SimulatedTransport tp;
  NopSecurity s;
  TestApp app[4];
  unique_ptr<TomBFTReplica> replica[4];
  for (int i = 0; i < 4; i += 1) {
    replica[i] = unique_ptr<TomBFTReplica>(
        new TomBFTReplica(c, i, true, &tp, s, &app[i]));
  }
  TomBFTSequencer seq(c, &tp, s, 0);
  TomBFTClient client(c, ReplicaAddress("localhost", "0"), &tp, s);

  string reply;
  client.Invoke("test", [&](const string &req, const string &arg_reply) {
    reply = arg_reply;
    tp.CancelAllTimers();
  });
  tp.Run();
  // ASSERT_STREQ(reply.c_str(), "reply: test");
}
