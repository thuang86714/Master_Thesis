#include <sys/time.h>

#include "lib/timeval.h"
#include "transaction/benchmark/benchclient.h"

using namespace dsnet::transaction::kvstore;

namespace dsnet {
namespace transaction {
namespace benchmark {

DEFINE_LATENCY(txn);

BenchClient::BenchClient(KVClient *client,
        NextTxnFn next_txn_fn, int duration, int interval)
    : done(false), commit_txns(0), total_txns(0),
    client_(client), next_txn_fn_(next_txn_fn),
    duration_(duration), interval_(interval)
{
    _Latency_Init(&txn_latency_, "txn");
}

void
BenchClient::Start()
{
    gettimeofday(&start_time_, nullptr);
    InvokeNextTxn();
}

void
BenchClient::InvokeNextTxn()
{
    Latency_Start(&txn_latency_);
    next_txn_fn_(client_,
            bind(&BenchClient::OnTxnComplete,
                this,
                std::placeholders::_1,
                std::placeholders::_2));
}

void
BenchClient::OnTxnComplete(const std::map<std::string, std::string> &results,
        bool commit)
{
    if (done) {
        return;
    }

    latencies[Latency_End(&txn_latency_)/1000]++;
    total_txns++;
    if (commit) {
        commit_txns++;
    }

    struct timeval end_time;
    gettimeofday(&end_time, nullptr);
    struct timeval diff = timeval_sub(end_time, start_time_);

    if (interval_ > 0) {
        uint64_t ms = (
                (
                 (end_time.tv_sec * 1000000 + end_time.tv_usec) / 1000
                ) / interval_) * interval_;
        throughputs[ms]++;
    }

    if (diff.tv_sec >= duration_) {
        done = true;
    } else {
        InvokeNextTxn();
    }
}

} // namespace benchmark
} // namespace transaction
} // namespace dsnet
