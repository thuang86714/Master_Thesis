#pragma once

#include <functional>
#include <map>

#include "lib/transport.h"
#include "lib/latency.h"
#include "transaction/apps/kvstore/client.h"

namespace dsnet {
namespace transaction {
namespace benchmark {

typedef std::function<void (dsnet::transaction::kvstore::KVClient *client,
        dsnet::transaction::kvstore::KVStoreCB cb)> NextTxnFn;

class BenchClient
{
public:
    BenchClient(dsnet::transaction::kvstore::KVClient *client,
            NextTxnFn next_txn_fn, int duration, int interval);
    void Start();

    bool done;
    std::map<uint64_t, int> latencies, throughputs;
    uint64_t commit_txns, total_txns;

private:
    void InvokeNextTxn();
    void OnTxnComplete(const std::map<std::string, std::string> &results, bool commit);

    dsnet::transaction::kvstore::KVClient *client_;
    NextTxnFn next_txn_fn_;
    int duration_, interval_;
    struct timeval start_time_;
    struct Latency_t txn_latency_;
};

} // namespace benchmark
} // namespace transaction
} // namespace dsnet
