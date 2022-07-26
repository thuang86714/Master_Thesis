// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/ycsb.cc:
 *   Benchmarking client for a distributed transactional kv-store.
 *
 **********************************************************************/

#include <vector>
#include <map>
#include <atomic>
#include <algorithm>

#include "lib/latency.h"
#include "lib/timeval.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "lib/dpdktransport.h"
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
static int rand_value();

static bool ready = false;
static double alpha = -1;
static double *zipf;
static int readportion = 50;
static int updateportion = 50;
static int rmwportion = 0;
static int nKeys = 1000;
static int nValues = 1000;
static bool indep = true;
static vector<string> keys;
static vector<string> values;

static void
YCSBNextTxn(KVClient *client, KVStoreCB cb)
{
    int ttype = rand() % 100;

    if (ttype < readportion) {
        string key = keys.at(rand_key());
        client->InvokeGetTxn(key, cb);
    } else if (ttype < readportion + updateportion) {
        string key = keys.at(rand_key());
        string value = values.at(rand_value());
        client->InvokePutTxn(key, value, cb);
    } else {
        string key1 = keys.at(rand_key());
        string key2 = keys.at(rand_key());
        string value1 = values.at(rand_value());
        string value2 = values.at(rand_value());
        client->InvokeRMWTxn(key1, key2, value1, value2, indep, cb);
    }
}

int
main(int argc, char **argv)
{
    const char *configPath = nullptr;
    const char *keysPath = nullptr;
    const char *valuesPath = nullptr;

    dsnet::Transport *transport;
    string host, dev, transport_cmdline, stats_file;
    int dev_port = 0;
    int core_id = 0;
    int n_transport_cores = 1;

    int nShards = 1;

    int duration = 10;
    int interval = 0;
    int n_threads = 1;

    protomode_t mode = PROTO_UNKNOWN;
    enum { TRANSPORT_UDP, TRANSPORT_DPDK } transport_type = TRANSPORT_UDP;

    int opt;
    while ((opt = getopt(argc, argv, "c:d:e:f:gh:i:j:J:k:m:N:p:r:s:t:u:v:w:x:z:Z:")) != -1) {
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

        case 'h': // host
            host = string(optarg);
            break;

        case 'e': // device
        {
            dev = string(optarg);
            break;
        }

        case 'x': // device port
        {
            char *strtol_ptr;
            dev_port = strtoul(optarg, &strtol_ptr, 10);
            if ((*optarg == '\0') || (*strtol_ptr != '\0')) {
                fprintf(stderr, "option -x requires a numeric arg\n");
            }
            break;
        }

        case 'Z': // transport command line
        {
            transport_cmdline = string(optarg);
            break;
        }

        case 'p':
        {
            if (strcasecmp(optarg, "udp") == 0) {
                transport_type = TRANSPORT_UDP;
            } else if (strcasecmp(optarg, "dpdk") == 0) {
                transport_type = TRANSPORT_DPDK;
            } else {
                fprintf(stderr, "unknown transport '%s'\n", optarg);
            }
            break;
        }

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

        case 'v': // Generated values path
        {
            valuesPath = optarg;
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

        case 'r': // read portion
        {
            char *strtolPtr;
            readportion = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || readportion < 0 || readportion > 100)
            {
                fprintf(stderr,
                        "option -r requires a numeric arg between 0 and 100\n");
            }
            break;
        }

        case 'u': // update portion
        {
            char *strtolPtr;
            updateportion = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || updateportion < 0 || updateportion > 100)
            {
                fprintf(stderr,
                        "option -u requires a numeric arg between 0 and 100\n");
            }
            break;
        }

        case 'w': // rmw portion
        {
            char *strtolPtr;
            rmwportion = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || rmwportion < 0 || rmwportion > 100)
            {
                fprintf(stderr,
                        "option -w requires a numeric arg between 0 and 100\n");
            }
            break;
        }

        case 'g': // general transactions
        {
            indep = false;
            break;
        }

        case 's': // statistics file
        {
            stats_file = string(optarg);
            break;
        }

        case 't': // number of benchmark threads
        {
            char *strtolPtr;
            n_threads = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || n_threads < 1)
            {
                fprintf(stderr,
                        "option -t requires a numeric arg >= 1\n");
            }
            break;
        }

        case 'j': // transport core id (for DPDK)
        {
            char *strtolPtr;
            core_id = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || core_id < 0)
            {
                fprintf(stderr,
                        "option -j requires a numeric arg >= 0\n");
            }
            break;
        }

        case 'J': // number of transport cores (for DPDK)
        {
            char *strtolPtr;
            n_transport_cores = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || n_transport_cores < 1)
            {
                fprintf(stderr,
                        "option -J requires a numeric arg >= 1\n");
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
        Panic("tpccClient requires -h option\n");
    }

    if (mode == PROTO_UNKNOWN) {
        Panic("option -m required");
    }

    if (readportion + updateportion + rmwportion != 100) {
        Panic("Workload portions should add up to 100");
    }

    // Initialize random number seed
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srand(tv.tv_usec);

    // Initialize transport
    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("unable to read configuration file: %s", configPath);
    }

    Configuration config(configStream);
    ReplicaAddress addr(host, "0", dev);

    switch (transport_type) {
        case TRANSPORT_UDP:
            transport = new dsnet::UDPTransport(0, 0);
            break;
        case TRANSPORT_DPDK:
            transport = new dsnet::DPDKTransport(dev_port, 0,
                    n_transport_cores, core_id, transport_cmdline);
            break;
    }

    // Read in the keys from a file.
    string inkey, invalue;
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, inkey);
        keys.push_back(inkey);
    }
    in.close();
    in.open(valuesPath);
    if (!in) {
        fprintf(stderr, "Could not read values from: %s\n", valuesPath);
        exit(0);
    }
    for (int i = 0; i < nValues; i++) {
        getline(in, invalue);
        values.push_back(invalue);
    }
    in.close();

    // Start benchmark
    std::vector<KVClient *> kv_clients;
    std::vector<Client *> proto_clients;
    std::vector<BenchClient *> bench_clients;
    for (int i = 0; i < n_threads; i++) {
        Client *pc;
        switch (mode) {
            case PROTO_ERIS:
                pc = new eris::ErisClient(config, addr, transport);
                break;
            case PROTO_GRANOLA:
                pc = new granola::GranolaClient(config, addr, transport);
                break;
            case PROTO_UNREPLICATED:
                pc =
                    new transaction::unreplicated::UnreplicatedClient(config,
                            addr, transport);
                break;
            case PROTO_SPANNER:
                pc = new spanner::SpannerClient(config, addr, transport);
                break;
            case PROTO_TAPIR:
                Panic("Currently not supporting TAPIR");
            default:
                Panic("Unknown protocol mode");
        }
        proto_clients.push_back(pc);
        KVClient *kc = new KVClient(pc, nShards);
        kv_clients.push_back(kc);
        BenchClient *bc = new BenchClient(kc,
                YCSBNextTxn, duration, interval);
        transport->Timer(0, [=]() { bc->Start(); });
        bench_clients.push_back(bc);
    }

    Timeout check_finish(transport, 100, [&]() {
        for (BenchClient *c : bench_clients) {
            if (!c->done) {
                return;
            }
        }
        Notice("All clients done.");

        // Combine results
        uint64_t agg_total_latency = 0,
                 agg_commit_txns = 0,
                 agg_total_txns = 0;
        std::map<uint64_t, int> agg_throughputs, agg_latency_dist;
        for (BenchClient *c : bench_clients) {
            agg_commit_txns += c->commit_txns;
            agg_total_txns += c->total_txns;
            for (const auto &kv : c->throughputs) {
                agg_throughputs[kv.first] += kv.second;
            }
            for (const auto &kv : c->latencies) {
                agg_latency_dist[kv.first] += kv.second;
                agg_total_latency += kv.first * kv.second;
            }
        }

        Notice("Completed %lu transactions in %.3f seconds",
               agg_commit_txns, (float)duration);
        Notice("Commit rate %.3f", (double)agg_commit_txns / agg_total_txns);

        Notice("Average latency is %lu us", agg_total_latency/agg_total_txns);
        enum class Mode { kMedian, k90, k95, k99 };
        Mode m = Mode::kMedian;
        uint64_t sum = 0;
        uint64_t median, p90, p95, p99;
        for (auto kv : agg_latency_dist) {
            sum += kv.second;
            switch (m) {
                case Mode::kMedian:
                    if (sum >= agg_total_txns / 2) {
                        median = kv.first;
                        Notice("Median latency is %ld", median);
                        m = Mode::k90;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k90:
                    if (sum >= agg_total_txns * 90 / 100) {
                        p90 = kv.first;
                        Notice("90th percentile latency is  latency is %ld", p90);
                        m = Mode::k95;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k95:
                    if (sum >= agg_total_txns * 95 / 100) {
                        p95 = kv.first;
                        Notice("95th percentile latency is  latency is %ld", p95);
                        m = Mode::k99;
                        // fall through
                    } else {
                        break;
                    }
                case Mode::k99:
                    if (sum >= agg_total_txns * 99 / 100) {
                        p99 = kv.first;
                        Notice("99th percentile latency is  latency is %ld", p99);
                        goto done;
                    } else {
                        break;
                    }
            }
        }
    done:
        if (stats_file.size() > 0) {
            std::ofstream fs(stats_file.c_str(), std::ios::out);
            fs << agg_commit_txns / ((float)duration) << std::endl;
            fs << median << " " << p90 << " " << p95 << " " << p99 << std::endl;
            for (const auto &kv : agg_latency_dist) {
                fs << kv.first << " " << kv.second << std::endl;
            }
            fs.close();
            if (agg_throughputs.size() > 0) {
                fs.open(stats_file.append("_tputs").c_str());
                for (const auto &kv : agg_throughputs) {
                    fs << kv.first << " " << kv.second << std::endl;
                }
                fs.close();
            }
        }
        exit(0);
    });

    check_finish.Start();
    transport->Run();

    for (BenchClient *c : bench_clients) {
        delete c;
    }
    for (KVClient *c : kv_clients) {
        delete c;
    }
    for (Client *c : proto_clients) {
        delete c;
    }

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

int rand_value()
{
    // Uniform selection of values.
    return (rand() % nValues);
}
