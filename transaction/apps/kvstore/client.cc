// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/client.cc:
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

#include "transaction/apps/kvstore/client.h"
#include "transaction/common/type.h"

namespace dsnet {
namespace transaction {
namespace kvstore {

KVClient::KVClient(Client *proto_client, uint32_t nshards)
	: proto_client_(proto_client), nshards_(nshards) { }

KVClient::~KVClient() { }

void
KVClient::InvokeKVTxn(const std::vector<KVOp> &kvops, bool indep, KVStoreCB cb)
{
    std::map<shardnum_t, string> requests;
    std::map<shardnum_t, proto::KVTxnMessage> msgs;
    std::map<std::string, std::string> results;
    bool ro = true;

    if (kvops.empty()) {
        cb(results, true);
        return;
    }

    // Group kv operations according to shards
    for (KVOp op : kvops) {
        shardnum_t shard = key_to_shard(op.key, nshards_);
        if (requests.find(shard) == requests.end()) {
            proto::KVTxnMessage message;
            msgs.insert(std::pair<shardnum_t, proto::KVTxnMessage>(shard, message));
        }

        proto::KVTxnMessage &message = msgs.at(shard);
        proto::GetMessage *getMessage;
        proto::PutMessage *putMessage;
        switch (op.op_type) {
        case KVOp::GET: {
            getMessage = message.add_gets();
            getMessage->set_key(op.key);
            break;
        }
        case KVOp::PUT: {
            ro = false;
            putMessage = message.add_puts();
            putMessage->set_key(op.key);
            putMessage->set_value(op.value);
            break;
        }
        default:
            Panic("Wrong KV operation type");
        }
    }

    // Construct transaction request for each shard
    for (auto &shard_txn : msgs) {
        string txn_str;

        shard_txn.second.SerializeToString(&txn_str);
        requests.insert(std::pair<shardnum_t, string>(shard_txn.first, txn_str));
    }

    cb_ = cb;
    clientarg_t arg = {.indep = indep, .ro = ro};
    this->proto_client_->Invoke(requests,
                std::bind(&KVClient::InvokeCallback,
                    this,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3),
                (void *)&arg);
}

void
KVClient::InvokeGetTxn(const std::string &key, KVStoreCB cb)
{
    std::vector<KVOp> kvops;
    kvops.push_back(KVOp{ .op_type = KVOp::GET, .key = key });
    InvokeKVTxn(kvops, true, cb);
}

void
KVClient::InvokePutTxn(const std::string &key, const std::string &value, KVStoreCB cb)
{
    std::vector<KVOp> kvops;
    kvops.push_back(KVOp{ .op_type = KVOp::PUT, .key = key, .value = value });
    InvokeKVTxn(kvops, true, cb);
}

void
KVClient::InvokeRMWTxn(const std::string &key1, const std::string &key2,
                       const std::string &value1, const std::string &value2,
                       bool indep, KVStoreCB cb)
{
    std::vector<KVOp> kvops;
    kvops.push_back(KVOp{ .op_type = KVOp::GET, .key = key1 });
    kvops.push_back(KVOp{ .op_type = KVOp::GET, .key = key2 });
    kvops.push_back(KVOp{ .op_type = KVOp::PUT, .key = key1, .value = value1 });
    kvops.push_back(KVOp{ .op_type = KVOp::PUT, .key = key2, .value = value2 });
    InvokeKVTxn(kvops, indep, cb);
}

void
KVClient::InvokeCallback(const std::map<shardnum_t, std::string> &requests,
        const std::map<shardnum_t, std::string> &replies,
        bool commit)
{
    std::map<std::string, std::string> results;

    for (const auto &reply : replies) {
        proto::KVTxnReplyMessage reply_msg;
        reply_msg.ParseFromString(reply.second);

        for (int i = 0; i < reply_msg.rgets_size(); i++) {
            results[reply_msg.rgets(i).key()] = reply_msg.rgets(i).value();
        }
    }

    cb_(results, commit);
}

} // namespace kvstore
} // namespace transaction
} // namespace dsnet
