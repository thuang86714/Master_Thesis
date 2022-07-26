/***********************************************************************
 *
 * pbft-test.cc:
 *   test cases for pbft protocol
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 * Copyright 2021 Sun Guangda      <sung@comp.nus.edu.sg>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <memory>

#include "common/client.h"
#include "common/replica.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/signature.h"
#include "lib/simtransport.h"
#include "lib/transport.h"
#include "replication/pbft/client.h"
#include "replication/pbft/replica.h"

using namespace dsnet;
using namespace dsnet::pbft;
using namespace dsnet::pbft::proto;
using std::map;
using std::pair;
using std::sprintf;
using std::string;
using std::unique_ptr;
using std::vector;

class PbftTestApp : public AppReplica {
 public:
  vector<string> opList;
  string LastOp() { return opList.back(); }

  PbftTestApp(){};
  ~PbftTestApp(){};

  void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                     void *arg = nullptr, void *ret = nullptr) override {
    opList.push_back(req);
    reply = "reply: " + req;
  }
};

Secp256k1Signer defaultSigner;
Secp256k1Verifier defaultVerifier(defaultSigner);
HomogeneousSecurity defaultSecurity(defaultSigner, defaultVerifier);

template <int numberClient>
struct System {
  Security &security;
  SimulatedTransport transport;
  PbftTestApp apps[4];
  unique_ptr<PbftReplica> *replicas;
  unique_ptr<PbftClient> *clients;

  System(Security &security) : security(security), transport(true) {
    map<int, vector<ReplicaAddress> > replicaAddrs = {
        {0,
         {{"localhost", "1509"},
          {"localhost", "1510"},
          {"localhost", "1511"},
          {"localhost", "1512"}}}};
    Configuration c(1, 4, 1, replicaAddrs);

    replicas = new unique_ptr<PbftReplica>[4];
    for (int i = 0; i < 4; i += 1) {
      replicas[i] = unique_ptr<PbftReplica>(
          new PbftReplica(c, i, true, &transport, security, &apps[i]));
    }
    clients = new unique_ptr<PbftClient>[numberClient];
    for (int i = 0; i < numberClient; i += 1) {
      clients[i] = unique_ptr<PbftClient>(new PbftClient(
          c, ReplicaAddress("localhost", "0"), &transport, security));
    }
  }

  System() : System(defaultSecurity) {}

  ~System() {
    delete[] replicas;
    delete[] clients;
  }
};

void OneClientMultiOp(int numberOp, Security &security) {
  System<1> system(security);
  Client &client = *system.clients[0];
  Transport &transport = system.transport;

  int opIndex = 0;
  std::function<void(const string &, const string &)> onResp;
  onResp = [&](const string &req, const string &reply) {
    char buf[100];
    sprintf(buf, "test%d", opIndex);
    ASSERT_EQ(req, buf);
    for (int i = 0; i < 4; i += 1) {
      ASSERT_EQ(system.apps[i].LastOp(), buf);
    }

    sprintf(buf, "reply: test%d", opIndex);
    ASSERT_EQ(reply, buf);
    opIndex += 1;
    if (opIndex < numberOp) {
      sprintf(buf, "test%d", opIndex);
      client.Invoke(buf, onResp);
    }
  };

  client.Invoke("test0", onResp);
  transport.Timer(1500, [&]() { transport.Stop(); });
  transport.Run();

  ASSERT_EQ(opIndex, numberOp);
}

TEST(Pbft, 1Op) {
  NopSecurity security;
  OneClientMultiOp(1, security);
}

TEST(Pbft, 1OpSign) { OneClientMultiOp(1, defaultSecurity); }

TEST(Pbft, 100Op) {
  NopSecurity security;
  OneClientMultiOp(100, security);
}

TEST(Pbft, 100OpSign) { OneClientMultiOp(100, defaultSecurity); }

using filter_t = std::function<bool(TransportReceiver *, std::pair<int, int>,
                                    TransportReceiver *, std::pair<int, int>,
                                    Message &, uint64_t &delay)>;

filter_t DisableRx(TransportReceiver *r) {
  return [=](TransportReceiver *src, pair<int, int> srcId,
             TransportReceiver *dst, pair<int, int> dstId, Message &msg,
             uint64_t &delay) { return dst != r; };
}

filter_t DisableTraffic(TransportReceiver *src, TransportReceiver *dst) {
  return [=](TransportReceiver *src_, pair<int, int> srcId,
             TransportReceiver *dst_, pair<int, int> dstId, Message &msg,
             uint64_t &delay) { return src != src_ || dst != dst_; };
}

TEST(Pbft, ResendPrePrepare) {
  System<1> system;
  Client &client = *system.clients[0];
  bool done = false;
  client.Invoke("warmup", [&](const string &, const string &) {
    // prevent replica rx, so no prepreare and client request is delivered
    for (int i = 1; i < 4; i += 1) {
      system.transport.AddFilter(i, DisableRx(system.replicas[i].get()));
    }
    client.Invoke("test", [&](const string &, const string &) { done = true; });
    system.transport.AddFilter(0, DisableRx(system.replicas[0].get()));

    system.transport.Timer(1000, [&]() {
      for (int i = 0; i < 4; i += 1) {
        system.transport.RemoveFilter(i);
      }
    });
  });
  system.transport.Timer(1800, [&]() { system.transport.Stop(); });
  system.transport.Run();
  ASSERT_FALSE(done);

  system.transport.Timer(800, [&]() { system.transport.Stop(); });
  system.transport.Run();
  ASSERT_TRUE(done);
}

TEST(Pbft, SimpleStateTransfer) {
  System<1> system;
  Client &client = *system.clients[0];
  bool done = false;
  client.Invoke("warmup", [&](const string &, const string &) {
    for (int i = 1; i < 4; i += 1) {
      for (int j = 1; j < 4; j += 1) {
        system.transport.AddFilter(
            (i << 2) | j,
            DisableTraffic(system.replicas[i].get(), system.replicas[j].get()));
      }
    }
    client.Invoke("test", [&](const string &, const string &) { done = true; });
    system.transport.Timer(100, [&]() {
      for (int i = 1; i < 4; i += 1) {
        for (int j = 1; j < 4; j += 1) {
          system.transport.RemoveFilter((i << 2) | j);
        }
      }
    });
  });
  system.transport.Timer(3000, [&]() { system.transport.Stop(); });
  system.transport.Run();
  ASSERT_TRUE(done);
}