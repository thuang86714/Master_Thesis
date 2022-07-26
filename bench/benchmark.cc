// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.cpp:
 *   simple replication benchmark client
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

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/timeval.h"

#include <sys/time.h>
#include <string>
#include <sstream>
#include <algorithm>

namespace dsnet {

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(Client &client, Transport &transport,
                                 int duration, uint64_t delay,
                                 int tputInterval)
    : tputInterval(tputInterval), client(client),
    transport(transport), duration(duration), delay(delay)
{
    if (delay != 0) {
        Notice("Delay between requests: %ld ms", delay);
    }
    done = false;
    completedOps = 0;
    _Latency_Init(&latency, "op");
}

void
BenchmarkClient::Start()
{
    n = 0;

    gettimeofday(&startTime, nullptr);
    SendNext();
}

void
BenchmarkClient::SendNext()
{
    std::ostringstream msg;
    msg << "request" << n;

    Latency_Start(&latency);
    client.Invoke(msg.str(), std::bind(&BenchmarkClient::OnReply,
                                       this,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
}

void
BenchmarkClient::OnReply(const string &request, const string &reply)
{
    if (done) {
        return;
    }

    uint64_t ns = Latency_End(&latency);
    latencies[ns/1000]++;
    completedOps++;

    gettimeofday(&endTime, NULL);
    struct timeval diff = timeval_sub(endTime, startTime);

    if (tputInterval > 0) {
        uint64_t ms = (((endTime.tv_sec*1000000+endTime.tv_usec)/1000)/tputInterval)*tputInterval;
        throughputs[ms]++;
    }

    if (diff.tv_sec >= duration) {
        Finish();
    } else {
        n++;
        if (delay == 0) {
            SendNext();
        } else {
            uint64_t rdelay = rand() % delay*2;
            transport.Timer(rdelay,
                    std::bind(&BenchmarkClient::SendNext, this));
        }
    }
}

void
BenchmarkClient::Finish()
{
    done = true;
    struct timeval diff = timeval_sub(endTime, startTime);
    Notice("Completed %ld requests in " FMT_TIMEVAL_DIFF " seconds",
           completedOps, VA_TIMEVAL_DIFF(diff));
}

} // namespace dsnet
