// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/kvClient.cc:
 *   Benchmarking client for a distributed transactional kv-store.
 *
 **********************************************************************/

#include <vector>
#include <map>
#include <set>
#include <algorithm>

#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "transaction/common/frontend/txnclientcommon.h"
#include "transaction/apps/kvstore/client.h"
#include "transaction/eris/client.h"
#include "transaction/granola/client.h"
#include "transaction/unreplicated/client.h"
#include "transaction/spanner/client.h"
#include "transaction/tapir/client.h"
#include "transaction/benchmark/header.h"
#include "transaction/benchmark/benchclient.h"

using namespace std;
using namespace dsnet;
using namespace dsnet::transaction;
using namespace dsnet::transaction::kvstore;
using namespace dsnet::transaction::benchmark;

DEFINE_LATENCY(op);

// Function to pick a random key according to some distribution.
static int rand_key();

static bool ready = false;
static double alpha = -1;
static double *zipf;

static int mptxnPer = 0; // percentage of multi-phase transactions (/100)
static int tLen = 10;
static int gLen = 2;
static int wPer = 50; // Out of 100

static vector<string> keys;
static int nKeys = 100;


static void
KVNextTxn(KVClient *client, KVStoreCB cb)
{
    vector<KVOp> ops;

    bool indep = true;
    if (mptxnPer > 0) {
        if (rand() % 100 < mptxnPer) {
            indep = false;
        }
    }
    int nkeys = tLen;
    if (!indep) {
        nkeys = gLen; // General transactions
    }
    for (int i = 0; i < nkeys; i++) {
        string key = keys[rand_key()];

        if (rand() % 100 < wPer) {
            ops.push_back(KVOp{ .op_type = KVOp::PUT, .key = key, .value = key });
        } else {
            ops.push_back(KVOp{ .op_type = KVOp::GET, .key = key });
        }
    }

    client->InvokeKVTxn(ops, indep, cb);
}

int
main(int argc, char **argv)
{
    const char *configPath = nullptr;
    const char *keysPath = nullptr;
    int duration = 10;
    int nShards = 1;
    int interval = 0;

    string host;

    protomode_t mode = PROTO_UNKNOWN;

    int opt;
    while ((opt = getopt(argc, argv, "c:d:h:N:l:w:k:f:m:z:p:g:i:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'd': // duration to run
        {
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (duration <= 0)) {
                fprintf(stderr, "option -d requires a numeric arg > 0\n");
            }
            break;
        }

        case 'h': // client host address
            host = string(optarg);
            break;

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (nShards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'l': // Length of each transaction (deterministic!)
        {
            char *strtolPtr;
            tLen = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (tLen <= 0)) {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;
        }

        case 'w': // Percentage of writes (out of 100)
        {
            char *strtolPtr;
            wPer = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (wPer < 0 || wPer > 100)) {
                fprintf(stderr, "option -w requires a arg b/w 0-100\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }

        case 'f': // Generated keys path
        {
            keysPath = optarg;
            break;
        }

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "eris") == 0) {
                mode = PROTO_ERIS;
            } else if (strcasecmp(optarg, "granola") == 0) {
                mode = PROTO_GRANOLA;
            } else if (strcasecmp(optarg, "unreplicated") == 0) {
                mode = PROTO_UNREPLICATED;
            } else if (strcasecmp(optarg, "spanner") == 0) {
                mode = PROTO_SPANNER;
            } else if (strcasecmp(optarg, "tapir") == 0) {
                mode = PROTO_TAPIR;
            } else {
                fprintf(stderr, "Unknown protocol mode %s\n", optarg);
            }
            break;
        }

        case 'z': // Zipf coefficient for key selection.
        {
            char *strtolPtr;
            alpha = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
            }
            break;
        }

        case 'p': // Percentage of multi-phase transactions
        {
            char *strtolPtr;
            mptxnPer = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || mptxnPer < 0 || mptxnPer > 100)
            {
                fprintf(stderr,
                        "option -p requires a numeric arg between 0 and 100\n");
            }
            break;
        }

        case 'g': // Length of multi-phase transactions
        {
            char *strtolPtr;
            gLen = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || gLen < 0)
            {
                fprintf(stderr,
                        "option -g requires a numeric arg > 0\n");
            }
            break;
        }

        case 'i': // Throughput measurement interval
        {
            char *strtolPtr;
            interval = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || interval <= 0)
            {
                fprintf(stderr,
                        "option -i requires a numeric arg > 0\n");
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (configPath == nullptr) {
        Panic("option -c required");
    }

    if (host.empty()) {
        Panic("option -h required");
    }

    if (mode == PROTO_UNKNOWN) {
        Panic("option -m required");
    }

    // Initialize random number seed
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srand(tv.tv_usec);

    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("unable to read configuration file: %s", configPath);
    }

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    // Initialize transport
    Configuration config(configStream);
    UDPTransport *transport = new UDPTransport();
    ReplicaAddress addr(host, "0");

    // Start benchmark clients
    KVClient *kc;
    Client *pc;
    BenchClient *bc;

    switch (mode) {
    case PROTO_ERIS: {
        pc = new eris::ErisClient(config, addr, transport);
        break;
    }
    case PROTO_GRANOLA: {
        pc = new granola::GranolaClient(config, addr, transport);
        break;
    }
    case PROTO_UNREPLICATED: {
        pc =
            new transaction::unreplicated::UnreplicatedClient(config, addr, transport);
        break;
    }
    case PROTO_SPANNER: {
        pc = new spanner::SpannerClient(config, addr, transport);
        break;
    }
    case PROTO_TAPIR: {
        Panic("Currently not supporting TAPIR");
    }
    default:
        Panic("Unknown protocol mode");
    }
    kc = new KVClient(pc, nShards);
    bc = new BenchClient(kc, KVNextTxn, duration, interval);
    transport->Timer(0, [=]() { bc->Start(); });

    Timeout check_finish(transport, 100, [&]() {
        if (!bc->done) {
            return;
        }
        Notice("All clients done.");

        // Combine results
        uint64_t total_latency = 0;
        for (const auto &kv : bc->latencies) {
            total_latency += kv.first * kv.second;
        }

        Notice("Completed %lu transactions in %.3f seconds",
               bc->commit_txns, (float)duration);
        Notice("Commit rate %.3f", (double)bc->commit_txns / bc->total_txns);

        Notice("Average latency is %lu us", total_latency/bc->total_txns);
        enum class Mode { kMedian, k90, k95, k99 };
        Mode m = Mode::kMedian;
        uint64_t sum = 0;
        uint64_t median, p90, p95, p99;
        for (auto kv : bc->latencies) {
            sum += kv.second;
            switch (m) {
                case Mode::kMedian:
                    if (sum >= bc->total_txns / 2) {
                        median = kv.first;
                        Notice("Median latency is %ld", median);
                        m = Mode::k90;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k90:
                    if (sum >= bc->total_txns * 90 / 100) {
                        p90 = kv.first;
                        Notice("90th percentile latency is  latency is %ld", p90);
                        m = Mode::k95;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k95:
                    if (sum >= bc->total_txns * 95 / 100) {
                        p95 = kv.first;
                        Notice("95th percentile latency is  latency is %ld", p95);
                        m = Mode::k99;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k99:
                    if (sum >= bc->total_txns * 99 / 100) {
                        p99 = kv.first;
                        Notice("99th percentile latency is  latency is %ld", p99);
                        goto done;
                    } else {
                        break;
                    }
            }
        }
done:
        exit(0);
    });

    check_finish.Start();
    transport->Run();

    delete bc;
    delete kc;
    delete pc;
    delete transport;

    return 0;
}

int rand_key()
{
    if (alpha < 0) {
        // Uniform selection of keys.
        return (rand() % nKeys);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i - 1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand()) / RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}
