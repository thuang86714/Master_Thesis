#pragma once

#include <mutex>
#include <rte_ether.h>
#include <rte_byteorder.h>
#include <rte_timer.h>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"

namespace dsnet {

class DPDKTransportAddress : public TransportAddress
{
public:
    DPDKTransportAddress(const std::string &s);
    DPDKTransportAddress(const struct rte_ether_addr &ether_addr,
                         rte_be32_t ip_addr,
                         rte_be16_t udp_addr);
    virtual DPDKTransportAddress * clone() const override;

private:
    struct rte_ether_addr ether_addr_;
    rte_be32_t ip_addr_;
    rte_be16_t udp_addr_;

    friend bool operator==(const DPDKTransportAddress &a,
                           const DPDKTransportAddress &b);
    friend bool operator<(const DPDKTransportAddress &a,
                          const DPDKTransportAddress &b);

    friend class DPDKTransport;
};

class DPDKTransport : public TransportCommon<DPDKTransportAddress>
{
public:
    DPDKTransport(int dev_port, double drop_rate = 0.0,
                  int n_cores = 1,
                  int core_id = 0,
                  const std::string &cmdline = "");
    virtual ~DPDKTransport();
    virtual void RegisterInternal(TransportReceiver *receiver,
                                  const ReplicaAddress *addr,
                                  int group_id, int replica_id) override;
    virtual void ListenOnMulticast(TransportReceiver *receiver,
                                   const Configuration &config) override;
    virtual void Run() override;
    virtual void Stop() override;
    virtual int Timer(uint64_t ms, timer_callback_t cb) override;
    virtual bool CancelTimer(int id) override;
    virtual void CancelAllTimers() override;
    virtual ReplicaAddress
    ReverseLookupAddress(const TransportAddress &addr) const override;

    void RunTransport();

private:
    struct DPDKTransportTimerInfo
    {
        DPDKTransport *transport;
        timer_callback_t cb;
        struct rte_timer timer;
        int id;
    };

    int dev_port_;
    double drop_rate_;
    int n_cores_;
    int core_id_;
    volatile enum {
        RUNNING,
        STOPPED,
    } status_;
    std::unordered_map<uint16_t, TransportReceiver *> receivers_;
    DPDKTransportAddress *multicast_addr_;
    std::vector<TransportReceiver *> multicast_receivers_;
    std::unordered_map<int, DPDKTransportTimerInfo *> timers_;
    std::mutex timers_lock_;
    int last_timer_id_;
    struct rte_mempool *pktmbuf_pool_;

    virtual bool SendMessageInternal(TransportReceiver *src,
                                     const DPDKTransportAddress &dst,
                                     const Message &m) override;
    virtual DPDKTransportAddress
    LookupAddressInternal(const ReplicaAddress &addr) const override;
    TransportReceiver *RouteToReceiver(const DPDKTransportAddress &addr);
    static void TimerCallback(struct rte_timer *timer, void *arg);
    void OnTimer(DPDKTransportTimerInfo *info);
};

} // namespace dsnet
