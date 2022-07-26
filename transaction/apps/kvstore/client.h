// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/client.h:
 *   A transactional key value store client.
 *
 * Copyright 2016 Jialin Li <lijl@cs.washington.edu>
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

#pragma once

#include <string>
#include <map>
#include <functional>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "common/client.h"
#include "transaction/apps/kvstore/kvstore-proto.pb.h"

namespace dsnet {
namespace transaction {
namespace kvstore {

typedef struct {
    enum OpType {
        GET,
        PUT
    };
    OpType op_type;
    std::string key;
    std::string value;
} KVOp;

// map: key -> value, bool: commit
typedef std::function<void (const std::map<std::string, std::string> &, bool)> KVStoreCB;

class KVClient
{
public:
    KVClient(Client *proto_client, uint32_t nshards);
    ~KVClient();

    void InvokeKVTxn(const std::vector<KVOp> &kvops, bool indep, KVStoreCB cb);

    void InvokeGetTxn(const std::string &key, KVStoreCB cb);
    void InvokePutTxn(const std::string &key, const std::string &value, KVStoreCB cb);
    void InvokeRMWTxn(const std::string &key1, const std::string &key2,
            const std::string &value1, const std::string &value2,
            bool indep, KVStoreCB cb);

    // Made public for testing
    shardnum_t key_to_shard(const std::string &key, const uint32_t nshards) {
        uint64_t hash = 5381;
        const char* str = key.c_str();
        for (unsigned int i = 0; i < key.length(); i++) {
            hash = ((hash << 5) + hash) + (uint64_t)str[i];
        }

        return (shardnum_t)(hash % nshards);
    };

private:
    Client *proto_client_;
    uint32_t nshards_;
    KVStoreCB cb_;

    void InvokeCallback(const std::map<shardnum_t, std::string> &requests,
                        const std::map<shardnum_t, std::string> &replies,
                        bool commit);
};

} // namespace kvstore
} // namespace transaction
} // namespace dsnet
