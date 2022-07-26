#include <arpa/inet.h>
#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_malloc.h>

#include "lib/dpdktransport.h"

namespace dsnet {

#define TIMER_RESOLUTION_MS 1
#define MAX_PKT_BURST 32
#define MEMPOOL_CACHE_SIZE 256
#define RTE_RX_DESC 4096
#define RTE_TX_DESC 4096
#define IPV4_HDR_SIZE 5
#define IPV4_TTL 0xFF

typedef uint32_t Preamble;
static const Preamble NONFRAG_MAGIC = 0x20050318;

DPDKTransportAddress::DPDKTransportAddress(const std::string &s)
{
    const char *p = s.data();
    ether_addr_ = *(struct rte_ether_addr *)p;
    p += sizeof(ether_addr_);
    ip_addr_ = *(rte_be32_t *)p;
    p += sizeof(ip_addr_);
    udp_addr_ = *(rte_be16_t *)p;
}

DPDKTransportAddress::DPDKTransportAddress(const struct rte_ether_addr &ether_addr,
                                           rte_be32_t ip_addr,
                                           rte_be16_t udp_addr)
    : ether_addr_(ether_addr), ip_addr_(ip_addr), udp_addr_(udp_addr) { }

DPDKTransportAddress *
DPDKTransportAddress::clone() const
{
    return new DPDKTransportAddress(*this);
}

bool
operator==(const DPDKTransportAddress &a, const DPDKTransportAddress &b)
{
    return (memcmp(&a.ether_addr_, &b.ether_addr_, sizeof(a.ether_addr_)) == 0 &&
            a.ip_addr_ == b.ip_addr_ &&
            a.udp_addr_ == b.udp_addr_);
}

bool
operator<(const DPDKTransportAddress &a, const DPDKTransportAddress &b)
{
    int r;
    if ((r = memcmp(&a.ether_addr_, &b.ether_addr_, sizeof(a.ether_addr_))) != 0)  {
        return r < 0;
    }
    if (a.ip_addr_ != b.ip_addr_) {
        return a.ip_addr_ < b.ip_addr_;
    }
    return a.udp_addr_ < b.udp_addr_;
}

static void
ConstructArguments(int argc, char **argv, int core_id, const std::string &cmdline)
{
    argv[0] = new char[strlen("command")+1];
    strcpy(argv[0], "command");
    argv[1] = new char[strlen("-l")+1];
    strcpy(argv[1], "-l");
    argv[2] = new char[16];
    sprintf(argv[2], "%d", core_id);
    argv[3] = new char[strlen("--proc-type=auto")+1];
    strcpy(argv[3], "--proc-type=auto");
    if (cmdline.length() > 0) {
        argv[4] = new char[cmdline.length()+1];
        strcpy(argv[4], cmdline.c_str());
    }
}

#define FLOW_TRANSPORT_PRIORITY 0
#define FLOW_DEFAULT_PRIORITY 1
#define FLOW_PATTERN_NUM 4
#define FLOW_ETH_TYPE_MASK 0xFFFF
#define FLOW_IPV4_PROTO_UDP 0x11
#define FLOW_IPV4_PROTO_MASK 0xFF
#define FLOW_UDP_PORT_MASK 0xFF00
#define FLOW_ACTION_NUM 2

static void
GenerateFlowRules(int dev_port, int n_cores)
{
    if (n_cores <= 1) {
        return;
    }

    {
        /* Default flow rule: drop */
        struct rte_flow_attr attr;
        memset(&attr, 0, sizeof(struct rte_flow_attr));
        attr.priority = FLOW_DEFAULT_PRIORITY;
        attr.ingress = 1;
        struct rte_flow_item patterns[2];
        memset(patterns, 0, sizeof(patterns));
        struct rte_flow_item_eth eth_spec;
        struct rte_flow_item_eth eth_mask;
        memset(&eth_spec, 0, sizeof(struct rte_flow_item_eth));
        memset(&eth_mask, 0, sizeof(struct rte_flow_item_eth));
        patterns[0].type = RTE_FLOW_ITEM_TYPE_ETH;
        patterns[0].spec = &eth_spec;
        patterns[0].mask = &eth_mask;
        patterns[1].type = RTE_FLOW_ITEM_TYPE_END;
        struct rte_flow_action actions[2];
        actions[0].type = RTE_FLOW_ACTION_TYPE_DROP;
        actions[1].type = RTE_FLOW_ACTION_TYPE_END;
        if (rte_flow_validate(dev_port, &attr, patterns, actions, nullptr) != 0) {
            Panic("Default flow rule is not valid");
        }
        if (rte_flow_create(dev_port, &attr, patterns, actions, nullptr) == nullptr) {
            Panic("rte_flow_create failed");
        }
    }

    /* Configure receive flow rule for each core */
    for (int core = 0; core < n_cores; core++) {
        // Each core gets one rx queue
        uint16_t rx_queue_id = core;

        /* Attributes */
        struct rte_flow_attr attr;
        memset(&attr, 0, sizeof(struct rte_flow_attr));
        attr.priority = FLOW_TRANSPORT_PRIORITY;
        attr.ingress = 1;

        /* Header match */
        // Ethernet: accept only IPv4 packets
        struct rte_flow_item patterns[FLOW_PATTERN_NUM];
        memset(patterns, 0, sizeof(patterns));
        struct rte_flow_item_eth eth_spec;
        struct rte_flow_item_eth eth_mask;
        memset(&eth_spec, 0, sizeof(struct rte_flow_item_eth));
        memset(&eth_mask, 0, sizeof(struct rte_flow_item_eth));
        eth_spec.type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
        eth_mask.type = rte_cpu_to_be_16(FLOW_ETH_TYPE_MASK);
        patterns[0].type = RTE_FLOW_ITEM_TYPE_ETH;
        patterns[0].spec = &eth_spec;
        patterns[0].mask = &eth_mask;
        // IPv4: accept only UDP packets
        struct rte_flow_item_ipv4 ip_spec;
        struct rte_flow_item_ipv4 ip_mask;
        memset(&ip_spec, 0, sizeof(struct rte_flow_item_ipv4));
        memset(&ip_mask, 0, sizeof(struct rte_flow_item_ipv4));
        ip_spec.hdr.next_proto_id = FLOW_IPV4_PROTO_UDP;
        ip_mask.hdr.next_proto_id = FLOW_IPV4_PROTO_MASK;
        patterns[1].type = RTE_FLOW_ITEM_TYPE_IPV4;
        patterns[1].spec = &ip_spec;
        patterns[1].mask = &ip_mask;
        // UDP: use the first byte of destination port to steer packet
        struct rte_flow_item_udp udp_spec;
        struct rte_flow_item_udp udp_mask;
        memset(&udp_spec, 0, sizeof(struct rte_flow_item_udp));
        memset(&udp_mask, 0, sizeof(struct rte_flow_item_udp));
        udp_spec.hdr.dst_port = rte_cpu_to_be_16(core << 8);
        udp_mask.hdr.dst_port = rte_cpu_to_be_16(FLOW_UDP_PORT_MASK);
        patterns[2].type = RTE_FLOW_ITEM_TYPE_UDP;
        patterns[2].spec = &udp_spec;
        patterns[2].mask = &udp_mask;

        patterns[3].type = RTE_FLOW_ITEM_TYPE_END;

        /* Actions: forward to queues */
        struct rte_flow_action actions[FLOW_ACTION_NUM];
        struct rte_flow_action_queue action_queue;
        memset(actions, 0, sizeof(actions));
        action_queue.index = rx_queue_id;
        actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
        actions[0].conf = &action_queue;
        actions[1].type = RTE_FLOW_ACTION_TYPE_END;

        /* Validate and install flow rules */
        if (rte_flow_validate(dev_port, &attr, patterns, actions, nullptr) != 0) {
            Panic("Flow rule is not valid");
        }
        if (rte_flow_create(dev_port, &attr, patterns, actions, nullptr) == nullptr) {
            Panic("rte_flow_create failed");
        }
    }
}

DPDKTransport::DPDKTransport(int dev_port,
        double drop_rate,
        int n_cores,
        int core_id,
        const std::string &cmdline)
    : dev_port_(dev_port), drop_rate_(drop_rate), n_cores_(n_cores), core_id_(core_id),
    status_(STOPPED), multicast_addr_(nullptr), last_timer_id_(0)
{
    // Initialize DPDK
    ASSERT(core_id >= 0);
    int argc = 4;
    if (cmdline.length() > 0) {
        argc++;
    }
    char **argv = new char*[argc];
    ConstructArguments(argc, argv, core_id_, cmdline);

    if (rte_eal_init(argc, argv) < 0) {
        Panic("rte_eal_init failed");
    }

    enum rte_proc_type_t proc_type = rte_eal_process_type();

    if (rte_eth_dev_count_avail() == 0) {
        Panic("No available Ethernet ports");
    }
    // Initialize pktmbuf pool
    char pool_name[32];
    sprintf(pool_name, "pktmbuf_pool");
    unsigned nb_mbufs = n_cores * (RTE_RX_DESC + RTE_TX_DESC);
    if (proc_type == RTE_PROC_PRIMARY) {
        pktmbuf_pool_ = rte_pktmbuf_pool_create(pool_name,
                                                nb_mbufs,
                                                MEMPOOL_CACHE_SIZE,
                                                0,
                                                RTE_MBUF_DEFAULT_BUF_SIZE,
                                                rte_socket_id());
    } else {
        pktmbuf_pool_ = rte_mempool_lookup(pool_name);
    }

    if (pktmbuf_pool_ == nullptr) {
        Panic("rte_pktmbuf_pool_create failed");
    }
    // Initialize timer library
    if (rte_timer_subsystem_init() != 0) {
        Panic("rte_timer_subsystem_init failed");
    }

    // Initialize port
    if (proc_type == RTE_PROC_PRIMARY) {
        struct rte_eth_conf port_conf;
        memset(&port_conf, 0, sizeof(port_conf));
        port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;
        port_conf.rx_adv_conf.rss_conf.rss_key = nullptr;
        if (n_cores > 1) {
            // Enable RSS
            port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_NONFRAG_IPV4_UDP;
        }

        struct rte_eth_dev_info dev_info;
        if (rte_eth_dev_info_get(dev_port_, &dev_info) != 0) {
            Panic("rte_eth_dev_info_get failed");
        }
        if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE) {
            port_conf.txmode.offloads |= DEV_TX_OFFLOAD_MBUF_FAST_FREE;
        }

        int num_rx_queues = n_cores;
        int num_tx_queues = n_cores;
        if (rte_eth_dev_configure(dev_port_,
                                  num_rx_queues,
                                  num_tx_queues,
                                  &port_conf) < 0) {
            Panic("rte_eth_dev_configure failed");
        }
        uint16_t nb_rxd = RTE_RX_DESC, nb_txd = RTE_TX_DESC;
        if (rte_eth_dev_adjust_nb_rx_tx_desc(dev_port_, &nb_rxd, &nb_txd) < 0) {
            Panic("rte_eth_dev_adjust_nb_rx_tx_desc failed");
        }

        // Initialize RX queues
        struct rte_eth_rxconf rxconf = dev_info.default_rxconf;
        rxconf.offloads = port_conf.rxmode.offloads;
        for (int i = 0; i < num_rx_queues; i++) {
            if (rte_eth_rx_queue_setup(dev_port_,
                                       i,
                                       nb_rxd,
                                       rte_eth_dev_socket_id(dev_port_),
                                       &rxconf,
                                       pktmbuf_pool_) < 0) {
                Panic("rte_eth_rx_queue_setup failed");
            }
        }

        // Initialize TX queues
        struct rte_eth_txconf txconf = dev_info.default_txconf;
        txconf.offloads = port_conf.txmode.offloads;
        for (int i = 0; i < num_tx_queues; i++) {
            if (rte_eth_tx_queue_setup(dev_port_,
                                       i,
                                       nb_txd,
                                       rte_eth_dev_socket_id(dev_port_),
                                       &txconf) < 0) {
                Panic("rte_eth_tx_queue_setup failed");
            }
        }

        // Start device
        if (rte_eth_dev_start(dev_port_) < 0) {
            Panic("rte_eth_dev_start failed");
        }
        if (rte_eth_promiscuous_enable(dev_port_) != 0) {
            Panic("rte_eth_promiscuous_enable failed");
        }

        // Create flow rules
        GenerateFlowRules(dev_port, n_cores);
    }
}

DPDKTransport::~DPDKTransport()
{
    delete multicast_addr_;
}

void
DPDKTransport::RegisterInternal(TransportReceiver *receiver,
                                const ReplicaAddress *addr,
                                int group_id, int replica_id)
{
    ASSERT(addr != nullptr);
    DPDKTransportAddress *da = new DPDKTransportAddress(LookupAddressInternal(*addr));

    // We use first byte of udp port to steer packet
    do {
        uint16_t udp_port = (rand() % 256) | (core_id_ << 8);
        da->udp_addr_ = rte_cpu_to_be_16(udp_port);
    } while (receivers_.count(da->udp_addr_) > 0);

    receiver->SetAddress(da);
    receivers_[da->udp_addr_] = receiver;
}

void
DPDKTransport::ListenOnMulticast(TransportReceiver *receiver,
                                 const Configuration &config)
{
    if (multicast_addr_ != nullptr) {
        return;
    }
    multicast_addr_ = LookupAddressInternal(*config.multicast()).clone();
    multicast_receivers_.push_back(receiver);
}

static int
DPDKMainThread(void *arg)
{
    DPDKTransport *transport = (DPDKTransport *)arg;
    transport->RunTransport();
    return 0;
}

void
DPDKTransport::Run()
{
    rte_eal_mp_remote_launch(DPDKMainThread, (void *)this, CALL_MAIN);
}

void
DPDKTransport::RunTransport()
{
    static uint64_t cycles_per_ms = rte_get_timer_hz() / 1000;
    static uint64_t timer_resolution_cycles = cycles_per_ms * TIMER_RESOLUTION_MS;

    uint16_t n_rx;
    struct rte_mbuf *pkt_burst[MAX_PKT_BURST];
    uint64_t cur_tsc, prev_tsc = 0;
    int rx_queue_id = core_id_;

    if (receivers_.empty()) {
        Panic("No transport receiver registered");
    }
    status_ = RUNNING;

    while (status_ == RUNNING) {
        cur_tsc = rte_rdtsc();
        if (cur_tsc - prev_tsc > timer_resolution_cycles) {
            rte_timer_manage();
            prev_tsc = cur_tsc;
        }
        n_rx = rte_eth_rx_burst(dev_port_,
                                rx_queue_id,
                                pkt_burst,
                                MAX_PKT_BURST);
        for (int i = 0; i < n_rx; i++) {
            struct rte_mbuf *m = pkt_burst[i];
            // Parse packet header
            struct rte_ether_hdr *ether_hdr;
            struct rte_ipv4_hdr *ip_hdr;
            struct rte_udp_hdr *udp_hdr;
            size_t offset = 0;
            ether_hdr = rte_pktmbuf_mtod_offset(m, struct rte_ether_hdr*, offset);
            if (ether_hdr->ether_type ==
                    rte_be_to_cpu_16(RTE_ETHER_TYPE_IPV4)) {
                offset += RTE_ETHER_HDR_LEN;
                ip_hdr = rte_pktmbuf_mtod_offset(m, struct rte_ipv4_hdr*, offset);
                if (ip_hdr->next_proto_id == IPPROTO_UDP) {
                    offset += (ip_hdr->version_ihl & RTE_IPV4_HDR_IHL_MASK) *
                        RTE_IPV4_IHL_MULTIPLIER;
                    udp_hdr = rte_pktmbuf_mtod_offset(m, struct rte_udp_hdr*, offset);
                    offset += sizeof(struct rte_udp_hdr);

                    // Deliver packet
                    TransportReceiver *receiver =
                        RouteToReceiver(DPDKTransportAddress(ether_hdr->d_addr,
                                                             ip_hdr->dst_addr,
                                                             udp_hdr->dst_port));
                    if (receiver != nullptr) {
                        void *msg_buf = rte_pktmbuf_mtod_offset(m, void*, offset);
                        char *ptr = (char *)msg_buf;
                        Preamble magic = *(Preamble *)ptr;
                        ptr += sizeof(Preamble);

                        if (magic == NONFRAG_MAGIC) {
                            // Construct source address
                            DPDKTransportAddress src(ether_hdr->s_addr,
                                                     ip_hdr->src_addr,
                                                     udp_hdr->src_port);
                            receiver->ReceiveMessage(src,
                                                     ptr,
                                                     rte_be_to_cpu_16(udp_hdr->dgram_len)
                                                     - sizeof(struct rte_udp_hdr)
                                                     - sizeof(Preamble));
                        }
                    }
                }
            }
            rte_pktmbuf_free(m);
        }
    }
}

void
DPDKTransport::Stop()
{
    status_ = STOPPED;
}

int
DPDKTransport::Timer(uint64_t ms, timer_callback_t cb)
{
    static const double hz = rte_get_timer_hz();
    std::lock_guard<std::mutex> lck(timers_lock_);
    DPDKTransportTimerInfo *info = new DPDKTransportTimerInfo();

    info->transport = this;
    info->cb = cb;
    rte_timer_init(&info->timer);
    info->id = ++last_timer_id_;
    timers_[info->id] = info;

    uint64_t ticks = (ms == 0) ? 0 : hz / (1000 / (double)ms);
    rte_timer_reset(&info->timer, ticks, SINGLE, core_id_, TimerCallback, info);

    return info->id;
}

bool
DPDKTransport::CancelTimer(int id)
{
    std::lock_guard<std::mutex> lck(timers_lock_);

    if (timers_.find(id) == timers_.end()) {
        return false;
    }

    DPDKTransportTimerInfo *info = timers_.at(id);
    if (info == nullptr) {
        return false;
    }

    rte_timer_stop(&info->timer);
    timers_.erase(info->id);
    delete info;

    return true;
}

void
DPDKTransport::CancelAllTimers()
{
    while (!timers_.empty()) {
        auto kv = timers_.begin();
        CancelTimer(kv->first);
    }
}

bool
DPDKTransport::SendMessageInternal(TransportReceiver *src,
                                   const DPDKTransportAddress &dst_addr,
                                   const Message &m)
{
    const DPDKTransportAddress &src_addr =
        static_cast<const DPDKTransportAddress&>(src->GetAddress());
    // Allocate mbuf
    struct rte_mbuf *mbuf = rte_pktmbuf_alloc(pktmbuf_pool_);
    if (mbuf == nullptr) {
        Panic("Failed to allocate rte_mbuf");
    }
    // Ethernet header
    struct rte_ether_hdr *ether_hdr =
        (struct rte_ether_hdr *)rte_pktmbuf_append(mbuf, RTE_ETHER_HDR_LEN);
    if (ether_hdr == nullptr) {
        Panic("Failed to allocate Ethernet header");
    }
    ether_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
    memcpy(&ether_hdr->d_addr, &dst_addr.ether_addr_, sizeof(struct rte_ether_addr));
    memcpy(&ether_hdr->s_addr, &src_addr.ether_addr_, sizeof(struct rte_ether_addr));
    // IP header
    struct rte_ipv4_hdr *ip_hdr;
    ip_hdr =
        (struct rte_ipv4_hdr *)rte_pktmbuf_append(mbuf,
                                                  IPV4_HDR_SIZE * RTE_IPV4_IHL_MULTIPLIER);
    if (ip_hdr == nullptr) {
        Panic("Failed to allocate IP header");
    }
    ip_hdr->version_ihl = (IPVERSION << 4) | IPV4_HDR_SIZE;
    ip_hdr->type_of_service = 0;
    ip_hdr->total_length = rte_cpu_to_be_16(IPV4_HDR_SIZE * RTE_IPV4_IHL_MULTIPLIER +
                                            sizeof(struct rte_udp_hdr) +
                                            sizeof(Preamble) +
                                            m.SerializedSize());
    ip_hdr->packet_id = 0;
    ip_hdr->fragment_offset = 0;
    ip_hdr->time_to_live = IPV4_TTL;
    ip_hdr->next_proto_id = IPPROTO_UDP;
    ip_hdr->hdr_checksum = 0;
    ip_hdr->src_addr = src_addr.ip_addr_;
    ip_hdr->dst_addr = dst_addr.ip_addr_;
    ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);
    /* UDP header */
    struct rte_udp_hdr *udp_hdr;
    udp_hdr = (struct rte_udp_hdr*)rte_pktmbuf_append(mbuf, sizeof(struct rte_udp_hdr));
    if (udp_hdr == nullptr) {
        Panic("Failed to allocate UDP header");
    }
    udp_hdr->src_port = src_addr.udp_addr_;
    udp_hdr->dst_port = dst_addr.udp_addr_;
    udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) +
                                          sizeof(Preamble) +
                                          m.SerializedSize());
    udp_hdr->dgram_cksum = 0;
    /* Datagram */
    void *dgram;
    dgram = rte_pktmbuf_append(mbuf, sizeof(Preamble) + m.SerializedSize());
    if (dgram == nullptr) {
        Panic("Failed to allocate data gram");
    }
    char *ptr = (char *)dgram;
    *(Preamble *)ptr = NONFRAG_MAGIC;
    ptr += sizeof(Preamble);
    m.Serialize(ptr);
    /* Send packet */
    int tx_queue = core_id_;
    if (rte_eth_tx_burst(dev_port_, tx_queue, &mbuf, 1) == 1) {
        return true;
    } else {
        rte_pktmbuf_free(mbuf);
        return false;
    }
}

DPDKTransportAddress
DPDKTransport::LookupAddressInternal(const ReplicaAddress &addr) const
{
    struct rte_ether_addr ether_addr;
    if (rte_ether_unformat_addr(addr.dev.data(), &ether_addr) != 0) {
        Panic("Failed to parse ethernet address");
    }
    rte_be32_t ip_addr;
    if (inet_pton(AF_INET, addr.host.data(), &ip_addr) != 1) {
        Panic("Failed to parse IP address");
    }
    uint16_t udp_port = uint16_t(stoul(addr.port));
    if (udp_port == 0) {
        // Assign a random udp port
        udp_port = rand() % 65535;
    }
    rte_be16_t udp_addr = rte_cpu_to_be_16(udp_port);
    return DPDKTransportAddress(ether_addr, ip_addr, udp_addr);
}

ReplicaAddress
DPDKTransport::ReverseLookupAddress(const TransportAddress &addr) const
{
    const DPDKTransportAddress *da = dynamic_cast<const DPDKTransportAddress *>(&addr);
    char host_buf[16], dev_buf[16];
    inet_ntop(AF_INET, &(da->ip_addr_), host_buf, 16);
    rte_ether_format_addr(dev_buf, 16, &(da->ether_addr_));
    return ReplicaAddress(std::string(host_buf),
                          std::to_string(rte_be_to_cpu_16(da->udp_addr_)),
                          std::string(dev_buf));
}

TransportReceiver *
DPDKTransport::RouteToReceiver(const DPDKTransportAddress &addr)
{
    if (multicast_addr_ != nullptr && addr == *multicast_addr_) {
        // For multicast packets, just deliver to the first receiver
        return multicast_receivers_.empty() ? nullptr :
                                              multicast_receivers_.front();
    }
    auto it = receivers_.find(addr.udp_addr_);
    return it == receivers_.end() ? nullptr :
                                    it->second;
}

void
DPDKTransport::TimerCallback(struct rte_timer *timer, void *arg)
{
    DPDKTransport::DPDKTransportTimerInfo *info =
        (DPDKTransport::DPDKTransportTimerInfo *)arg;
    info->transport->OnTimer(info);
}

void
DPDKTransport::OnTimer(DPDKTransportTimerInfo *info)
{
    {
        std::lock_guard<std::mutex> lck(timers_lock_);
        timers_.erase(info->id);
    }

    info->cb();
    delete info;
}

} // namespace dsnet
