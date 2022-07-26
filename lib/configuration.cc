// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * configuration.cc:
 *   Representation of a replica group configuration, i.e. the number
 *   and list of replicas in the group
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>

namespace dsnet {

ReplicaAddress ParseReplicaAddress(const char *);

ReplicaAddress::ReplicaAddress(const string &host, const string &port,
                               const string &dev)
    : host(host), port(port), dev(dev)
{

}

ReplicaAddress::ReplicaAddress(const string &addr)
{
    size_t start = 0, end;
    auto Parse = [&] () {
        end = addr.find('|', start);
        size_t s = start;
        start = end + 1;
        return addr.substr(s, end-s);
    };

    host = Parse();
    port = Parse();
    dev = Parse();
}

string
ReplicaAddress::Serialize() const
{
    string s;
    s.append(host);
    s.append("|");
    s.append(port);
    s.append("|");
    s.append(dev);
    return s;
}

bool
ReplicaAddress::operator==(const ReplicaAddress &other) const {
    return (host == other.host) &&
           (port == other.port) &&
           (dev == other.dev);
}


Configuration::Configuration(const Configuration &c)
    : g(c.g), n(c.n), f(c.f), replicas_(c.replicas_), sequencers_(c.sequencers_)
{
    multicast_ = c.multicast_ == nullptr ?
        nullptr :
        new ReplicaAddress(*c.multicast_);
    fc_ = c.fc_ == nullptr ?
        nullptr :
        new ReplicaAddress(*c.fc_);
}

Configuration::Configuration(int g, int n, int f,
                             const std::map<int, std::vector<ReplicaAddress>> &replicas,
                             const ReplicaAddress *multicast,
                             const std::vector<ReplicaAddress> &sequencers,
                             const ReplicaAddress *fc)
    : g(g), n(n), f(f), replicas_(replicas), sequencers_(sequencers)
{
    multicast_ = multicast == nullptr ?
        nullptr :
        new ReplicaAddress(*multicast);
    fc_ = fc == nullptr ?
        nullptr :
        new ReplicaAddress(*fc);
}

Configuration::Configuration(std::ifstream &file)
{
    f = -1;
    multicast_ = nullptr;
    fc_ = nullptr;
    int group = -1;

    while (!file.eof()) {
        // Read a line
        string line;
        getline(file, line);;
        // Ignore comments
        if ((line.size() == 0) || (line[0] == '#')) {
            continue;
        }

        // Get the command
        // This is pretty horrible, but C++ does promise that &line[0]
        // is going to be a mutable contiguous buffer...
        char *cmd = strtok(&line[0], " \t");

        if (strcasecmp(cmd, "f") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (!arg) {
                Panic ("'f' configuration line requires an argument");
            }
            char *strtolPtr;
            f = strtoul(arg, &strtolPtr, 0);
            if ((*arg == '\0') || (*strtolPtr != '\0')) {
                Panic("Invalid argument to 'f' configuration line");
            }
        } else if (strcasecmp(cmd, "group") == 0) {
            group++;
        } else if (strcasecmp(cmd, "replica") == 0) {
            if (group < 0) {
                group = 0;
            }
            replicas_[group].push_back(ParseReplicaAddress("replica"));
        } else if (strcasecmp(cmd, "multicast") == 0) {
            multicast_ = new ReplicaAddress(ParseReplicaAddress("multicast"));
        } else if (strcasecmp(cmd, "fc") == 0) {
            fc_ = new ReplicaAddress(ParseReplicaAddress("fc"));
        } else if (strcasecmp(cmd, "sequencer") == 0) {
            sequencers_.push_back(ParseReplicaAddress("sequencer"));
        } else {
            Panic("Unknown configuration directive: %s", cmd);
        }
    }

    g = replicas_.size();
    n = replicas_[0].size();

    // Sanity checks
    if (g == 0) {
        Panic("Configuration did not specify any groups");
    }
    if (n == 0) {
        Panic("Configuration did not specify any replicas");
    }
    for (const auto &kv : replicas_) {
        if (kv.second.size() != (size_t)n) {
            Panic("All groups must contain the same number of replicas.");
        }
    }
    if (f == -1) {
        Panic("Configuration did not specify a 'f' parameter");
    }
}

Configuration::~Configuration()
{
    delete multicast_;
    delete fc_;
}

const ReplicaAddress &
Configuration::replica(int group, int id) const
{
    return replicas_.at(group).at(id);
}

int
Configuration::NumSequencers() const
{
    return sequencers_.size();
}

const ReplicaAddress &
Configuration::sequencer(int index) const
{
    if (sequencers_.empty()) {
        Panic("No sequencer address specified in configuration");
    }
    index = (unsigned long)index >= sequencers_.size() ? sequencers_.size() - 1 : index;
    return sequencers_.at(index);
}

const ReplicaAddress *
Configuration::multicast() const
{
    return multicast_;
}

const ReplicaAddress *
Configuration::fc() const
{
    return fc_;
}

int
Configuration::QuorumSize() const
{
    return f+1;
}

int
Configuration::FastQuorumSize() const
{
    return f + (f+1)/2 + 1;
}

bool
Configuration::operator==(const Configuration &other) const
{
    if ((g != other.g) ||
            (n != other.n) ||
            (f != other.f) ||
            (replicas_ != other.replicas_) ||
            ((multicast_ == nullptr && other.multicast_ != nullptr) ||
             (multicast_ != nullptr && other.multicast_ == nullptr)) ||
            ((fc_ == nullptr && other.fc_ != nullptr) ||
             (fc_ != nullptr && other.fc_ == nullptr))) {
        return false;
    }

    if (multicast_ != nullptr) {
        if (*multicast_ != *other.multicast_) {
            return false;
        }
    }

    if (fc_ != nullptr) {
        if (*fc_ != *other.fc_) {
            return false;
        }
    }

    return true;
}

/* Utility functions */

ReplicaAddress
ParseReplicaAddress(const char *name)
{
    char *arg = strtok(nullptr, " \t");
    if (!arg) {
        Panic("'%s' configuration line requires an argument", name);
    }

    char *host = strtok(arg, ":");
    char *port = strtok(nullptr, "|");
    char *dev = strtok(nullptr, "");
    if (!host || !port) {
        Panic("Configuration line format: '%s host:port[|dev]'", name);
    }

    return ReplicaAddress(string(host), string(port),
                          dev == nullptr ? "" : string(dev));
}

} // namespace dsnet
