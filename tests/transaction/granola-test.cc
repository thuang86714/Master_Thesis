// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tests/granola-test.cc:
 *   Test cases for Granola transaction protocol (using key value store).
 *
 * Copyright 2016 Jialin Li    <lijl@cs.washington.edu>
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

#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/simtransport.h"

#include "transaction/common/frontend/txnclientcommon.h"
#include "transaction/granola/client.h"
#include "transaction/apps/kvstore/client.h"
#include "transaction/granola/server.h"
#include "transaction/apps/kvstore/txnserver.h"

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include <vector>
#include <utility>
#include <random>

using namespace dsnet;
using namespace dsnet::transaction;
using namespace dsnet::transaction::kvstore;
using namespace dsnet::transaction::granola;

class GranolaTest : public ::testing::Test
{
protected:
    /* Sharded servers. Each shard has 3 replicas. */
    std::map<int, std::vector<TxnServer *> > txnServers;
    std::map<int, std::vector<GranolaServer *> > protoServers;

    KVClient *kvClient;
    TxnClient *txnClient;
    Client *protoClient;
    SimulatedTransport *transport;
    Configuration *config;
    int requestNum;
    int nShards;

    virtual void SetUp() {
        // Setup all the node addresses
        std::map<int, std::vector<ReplicaAddress> > nodeAddrs =
        {
            {
                0,
                {
                    { "localhost", "12300" },
                    { "localhost", "12301" },
                    { "localhost", "12302" }
                }
            },
            {
                1,
                {
                    { "localhost", "12310" },
                    { "localhost", "12311" },
                    { "localhost", "12312" }
                }
            },
            {
                2,
                {
                    { "localhost", "12320" },
                    { "localhost", "12321" },
                    { "localhost", "12322" }
                }
            }
        };
        nShards = nodeAddrs.size();

        this->config = new Configuration(nShards, 3, 1, nodeAddrs);

        this->transport = new SimulatedTransport(true);

        // Create server replicas for each shard
        for (auto& kv : nodeAddrs) {
            int shardIdx = kv.first;
            this->txnServers[shardIdx] = std::vector<TxnServer *>();
            this->protoServers[shardIdx] = std::vector<GranolaServer *>();
            for (int i = 0; i < config->n; i++) {
                KVStoreTxnServerArg arg;
                arg.keyPath = nullptr;
                arg.retryLock = false;
                TxnServer *txnserver = new KVTxnServer(arg);
                GranolaServer *protoserver = new GranolaServer(*config, shardIdx, i, true, transport, txnserver);

                this->txnServers[shardIdx].push_back(txnserver);
                this->protoServers[shardIdx].push_back(protoserver);
            }
        }

        // Create client
        uint64_t client_id = 0;
        while (client_id == 0) {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<uint64_t> dis;
            client_id = dis(gen);
        }

        this->protoClient = new GranolaClient(*config,
                                              ReplicaAddress("localhost", "0"),
                                              transport, client_id);
        this->txnClient = new TxnClientCommon(transport, protoClient);
        this->kvClient = new KVClient(txnClient, nShards);
    }

    virtual void TearDown() {
        this->transport->Stop();
        delete this->kvClient;
        delete this->protoClient;

        for (auto &kv : txnServers) {
            for (auto server : kv.second) {
                delete server;
            }
        }
        txnServers.clear();
        for (auto &kv : protoServers) {
            for (auto protoserver : kv.second) {
                delete protoserver;
            }
        }
        protoServers.clear();
        delete transport;
        delete config;
    }
};

TEST_F(GranolaTest, SingleShardIndepWriteReadTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;
    KVOp_t putOp;
    putOp.opType = KVOp_t::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    kvClient->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 0);

    KVOp_t getOp;
    getOp.opType = KVOp_t::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    kvClient->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(results.find("k1") != results.end());
    EXPECT_EQ(results.at("k1"), "v1");
}

TEST_F(GranolaTest, MultiShardsIndepWriteReadTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    // First load keys ki with values vi
    KVOp_t putOp;
    KVOp_t getOp;
    putOp.opType = KVOp_t::PUT;
    getOp.opType = KVOp_t::GET;
    char buf [32], buf2 [32];
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        putOp.key = buf;
        sprintf(buf, "v%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);

    // Get keys ki and also update ki to vvi
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
        putOp.key = buf;
        sprintf(buf, "vv%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "v%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }

    // Get keys ki again
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "vv%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }
}

TEST_F(GranolaTest, SimpleTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    KVOp_t putOp;
    KVOp_t getOp;
    putOp.opType = KVOp_t::PUT;
    getOp.opType = KVOp_t::GET;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);
    putOp.key = "k2";
    putOp.value = "v2";
    kvops.push_back(putOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);

    kvops.clear();
    results.clear();
    getOp.key = "k1";
    kvops.push_back(getOp);
    getOp.key = "k2";
    kvops.push_back(getOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results.at("k1"), "v1");
    EXPECT_EQ(results.at("k2"), "v2");

    kvops.clear();
    results.clear();
    putOp.key = "k1";
    putOp.value = "V1";
    kvops.push_back(putOp);
    putOp.key = "k0";
    putOp.value = "v0";
    kvops.push_back(putOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);
}

TEST_F(GranolaTest, GeneralTransactionTest) {
    // Set servers into locking mode
    for (const auto &kv : protoServers) {
        for (auto server : kv.second) {
            server->SetMode(true);
        }
    }
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    // First load keys ki with values vi
    KVOp_t putOp;
    KVOp_t getOp;
    putOp.opType = KVOp_t::PUT;
    getOp.opType = KVOp_t::GET;
    char buf [32], buf2 [32];
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        putOp.key = buf;
        sprintf(buf, "v%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);

    // Get keys ki and also update ki to vvi
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
        putOp.key = buf;
        sprintf(buf, "vv%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "v%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }

    // Get keys ki again
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "vv%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }
}

