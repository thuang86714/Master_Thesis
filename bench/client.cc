// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * client.cpp:
 *   test instantiation of a client application
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#include "common/client.h"

#include <stdlib.h>
#include <unistd.h>

#include <fstream>

#include "bench/benchmark.h"
#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/dpdktransport.h"
#include "lib/message.h"
#include "lib/signature.h"
#include "lib/udptransport.h"
#include "replication/fastpaxos/client.h"
#include "replication/nopaxos/client.h"
#include "replication/pbft/client.h"
#include "replication/unreplicated/client.h"
#include "replication/vr/client.h"
#include "replication/tombft/client.h"

static void Usage(const char *progName) {
  fprintf(stderr,
          "usage: %s [-n requests] [-t threads] [-w warmup-secs] [-s "
          "stats-file] [-d delay-ms] [-u duration-sec] [-p udp|dpdk] [-v "
          "device] [-x device-port] [-z transport-cmdline] -c conf-file -h "
          "host-address -m unreplicated|vr|fastpaxos|nopaxos\n",
          progName);
  exit(1);
}

void PrintReply(const string &request, const string &reply) {
  Notice("Request succeeded; got response %s", reply.c_str());
}

int main(int argc, char **argv) {
  const char *configPath = NULL;
  int numClients = 1;
  int duration = 1;
  uint64_t delay = 0;
  int tputInterval = 0;
  std::string host, dev, transport_cmdline;
  int dev_port = 0;
  int n_transport_cores = 1;
  int core_id = 0;

  enum {
    PROTO_UNKNOWN,
    PROTO_UNREPLICATED,
    PROTO_VR,
    PROTO_FASTPAXOS,
    PROTO_SPEC,
    PROTO_NOPAXOS,
    PROTO_PBFT,
    PROTO_TOMBFT,
  } proto = PROTO_UNKNOWN;

  enum { TRANSPORT_UDP, TRANSPORT_DPDK } transport_type = TRANSPORT_UDP;

  string statsFile;

  // Parse arguments
  int opt;
  while ((opt = getopt(argc, argv, "c:d:h:s:m:t:i:u:p:v:x:z:j:J:")) != -1) {
    switch (opt) {
      case 'c':
        configPath = optarg;
        break;

      case 'd': {
        char *strtolPtr;
        delay = strtoul(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0')) {
          fprintf(stderr, "option -d requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;
      }

      case 'h':
        host = std::string(optarg);
        break;

      case 'v':
        dev = std::string(optarg);
        break;

      case 'x': {
        char *strtol_ptr;
        dev_port = strtoul(optarg, &strtol_ptr, 10);
        if ((*optarg == '\0') || (*strtol_ptr != '\0')) {
          fprintf(stderr, "option -x requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;
      }

      case 's':
        statsFile = string(optarg);
        break;

      case 'm':
        if (strcasecmp(optarg, "unreplicated") == 0) {
          proto = PROTO_UNREPLICATED;
        } else if (strcasecmp(optarg, "vr") == 0) {
          proto = PROTO_VR;
        } else if (strcasecmp(optarg, "fastpaxos") == 0) {
          proto = PROTO_FASTPAXOS;
        } else if (strcasecmp(optarg, "nopaxos") == 0) {
          proto = PROTO_NOPAXOS;
        } else if (strcasecmp(optarg, "tombft") == 0) {
          proto = PROTO_TOMBFT;
        } else if (strcasecmp(optarg, "pbft") == 0) {
          proto = PROTO_PBFT;
        } else {
          fprintf(stderr, "unknown mode '%s'\n", optarg);
          Usage(argv[0]);
        }
        break;

      case 'u': {
        char *strtolPtr;
        duration = strtoul(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0') || (duration <= 0)) {
          fprintf(stderr, "option -n requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;
      }

      case 't': {
        char *strtolPtr;
        numClients = strtoul(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0') || (numClients <= 0)) {
          fprintf(stderr, "option -t requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;
      }

      case 'i': {
        char *strtolPtr;
        tputInterval = strtoul(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0')) {
          fprintf(stderr, "option -d requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;
      }

      case 'p':
        if (strcasecmp(optarg, "udp") == 0) {
          transport_type = TRANSPORT_UDP;
        } else if (strcasecmp(optarg, "dpdk") == 0) {
          transport_type = TRANSPORT_DPDK;
        } else {
          fprintf(stderr, "unknown transport '%s'\n", optarg);
          Usage(argv[0]);
        }
        break;

      case 'z':
        transport_cmdline = std::string(optarg);
        break;

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
        Usage(argv[0]);
        break;
    }
  }

  if (!configPath) {
    fprintf(stderr, "option -c is required\n");
    Usage(argv[0]);
  }
  if (host.empty()) {
    fprintf(stderr, "option -h is required\n");
    Usage(argv[0]);
  }
  if (proto == PROTO_UNKNOWN) {
    fprintf(stderr, "option -m is required\n");
    Usage(argv[0]);
  }

  // Load configuration
  std::ifstream configStream(configPath);
  if (configStream.fail()) {
    fprintf(stderr, "unable to read configuration file: %s\n", configPath);
    Usage(argv[0]);
  }
  dsnet::Configuration config(configStream);

  dsnet::Transport *transport;
  switch (transport_type) {
    case TRANSPORT_UDP:
      transport = new dsnet::UDPTransport(0, 0);
      break;
    case TRANSPORT_DPDK:
      transport = new dsnet::DPDKTransport(dev_port, 0,
                                           n_transport_cores, core_id,
                                           transport_cmdline);
      break;
  }

  std::vector<dsnet::Client *> clients;
  std::vector<dsnet::BenchmarkClient *> benchClients;
  dsnet::ReplicaAddress addr(host, "0", dev);

  dsnet::Secp256k1Signer signer;
  dsnet::Secp256k1Verifier verifier(signer);
  dsnet::Signer seq_signer;
  dsnet::Verifier seq_verifier;
  dsnet::HomogeneousSecurity security(signer, verifier, seq_signer,
                                      seq_verifier);
  // dsnet::NopSecurity security;
  for (int i = 0; i < numClients; i++) {
    dsnet::Client *client;
    switch (proto) {
      case PROTO_UNREPLICATED:
        client = new dsnet::unreplicated::UnreplicatedClient(config, addr,
                                                             transport);
        break;

      case PROTO_VR:
        client = new dsnet::vr::VRClient(config, addr, transport);
        break;

      case PROTO_FASTPAXOS:
        client = new dsnet::fastpaxos::FastPaxosClient(config, addr, transport);
        break;

      case PROTO_NOPAXOS:
        client = new dsnet::nopaxos::NOPaxosClient(config, addr, transport);
        break;

      case PROTO_PBFT:
        client = new dsnet::pbft::PbftClient(config, addr, transport, security);
        break;

      case PROTO_TOMBFT:
        client = new dsnet::tombft::TomBFTClient(config, addr, transport, security);
        break;

      default:
        NOT_REACHABLE();
    }

    dsnet::BenchmarkClient *bench = new dsnet::BenchmarkClient(
        *client, *transport, duration, delay, tputInterval);

    transport->Timer(0, [=]() { bench->Start(); });
    clients.push_back(client);
    benchClients.push_back(bench);
  }

  dsnet::Timeout checkTimeout(transport, 100, [&]() {
    for (auto x : benchClients) {
      if (!x->done) {
        return;
      }
    }
    Notice("All clients done.");

    Latency_t sum;
    _Latency_Init(&sum, "total");
    std::map<int, int> agg_latencies;
    std::map<uint64_t, int> agg_throughputs;
    uint64_t agg_ops = 0;
    for (unsigned int i = 0; i < benchClients.size(); i++) {
      Latency_Sum(&sum, &benchClients[i]->latency);
      for (const auto &kv : benchClients[i]->latencies) {
        agg_latencies[kv.first] += kv.second;
      }
      for (const auto &kv : benchClients[i]->throughputs) {
        agg_throughputs[kv.first] += kv.second * (1000 / tputInterval);
      }
      agg_ops += benchClients[i]->completedOps;
    }
    Latency_Dump(&sum);

    Notice("Total throughput is %ld ops/sec", agg_ops / duration);
    enum class Mode { kMedian, k90, k95, k99 };
    uint64_t count = 0;
    int median, p90, p95, p99;
    Mode mode = Mode::kMedian;
    for (const auto &kv : agg_latencies) {
      count += kv.second;
      switch (mode) {
        case Mode::kMedian:
          if (count >= agg_ops / 2) {
            median = kv.first;
            Notice("Median latency is %d us", median);
            mode = Mode::k90;
            // fall through
          } else {
            break;
          }
        case Mode::k90:
          if (count >= agg_ops * 90 / 100) {
            p90 = kv.first;
            Notice("90th percentile latency is %d us", p90);
            mode = Mode::k95;
            // fall through
          } else {
            break;
          }
        case Mode::k95:
          if (count >= agg_ops * 95 / 100) {
            p95 = kv.first;
            Notice("95th percentile latency is %d us", p95);
            mode = Mode::k99;
            // fall through
          } else {
            break;
          }
        case Mode::k99:
          if (count >= agg_ops * 99 / 100) {
            p99 = kv.first;
            Notice("99th percentile latency is %d us", p99);
            goto done;
          } else {
            break;
          }
      }
    }

  done:
    if (statsFile.size() > 0) {
      std::ofstream fs(statsFile.c_str(), std::ios::out);
      fs << agg_ops / duration << std::endl;
      fs << median << " " << p90 << " " << p95 << " " << p99 << std::endl;
      for (const auto &kv : agg_latencies) {
        fs << kv.first << " " << kv.second << std::endl;
      }
      fs.close();
      if (agg_throughputs.size() > 0) {
        fs.open(statsFile.append("_tputs").c_str());
        for (const auto &kv : agg_throughputs) {
          fs << kv.first << " " << kv.second << std::endl;
        }
        fs.close();
      }
    }
    exit(0);
  });
  checkTimeout.Start();

  transport->Run();

  delete transport;
}
