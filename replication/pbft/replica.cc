#include "replication/pbft/replica.h"

#include "common/pbmessage.h"
#include "common/replica.h"
#include "lib/message.h"
#include "lib/signature.h"
#include "lib/transport.h"
#include "replication/pbft/pbft-proto.pb.h"

#define RDebug(fmt, ...) Debug("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, this->replicaIdx, ##__VA_ARGS__)

#define PROTOCOL_FMT "[protocol] [%s] >> view = %ld, seq = %ld"

using namespace std;

namespace dsnet {
namespace pbft {

using namespace proto;

PbftReplica::PbftReplica(const Configuration &config, int myIdx,
                         bool initialize, Transport *transport,
                         const Security &sec, AppReplica *app)
    : Replica(config, 0, myIdx, initialize, transport, app),
      security(sec),
      lastExecuted(0),
      log(false),
      prepareSet(2 * config.f),
      commitSet(2 * config.f + 1) {
  if (!initialize) NOT_IMPLEMENTED();

  this->status = STATUS_NORMAL;
  this->view = 0;
  this->seqNum = 0;

  // 1h timeout to make sure no one ever wants to change view
  viewChangeTimeout = new Timeout(transport, 3600 * 1000,
                                  bind(&PbftReplica::OnViewChange, this));
}

void PbftReplica::ReceiveMessage(const TransportAddress &remote, void *buf,
                                 size_t size) {
  static ToReplicaMessage replica_msg;
  static PBMessage m(replica_msg);

  m.Parse(buf, size);

  switch (replica_msg.msg_case()) {
    case ToReplicaMessage::MsgCase::kRequest:
      HandleRequest(remote, replica_msg.request());
      break;
    case ToReplicaMessage::MsgCase::kPrePrepare:
      HandlePrePrepare(remote, replica_msg.pre_prepare());
      break;
    case ToReplicaMessage::MsgCase::kPrepare:
      HandlePrepare(remote, replica_msg.prepare());
      break;
    case ToReplicaMessage::MsgCase::kCommit:
      HandleCommit(remote, replica_msg.commit());
      break;
    case ToReplicaMessage::MsgCase::kStateTransferRequest:
      HandleStateTransferRequest(remote, replica_msg.state_transfer_request());
      break;
    default:
      RPanic("Received unexpected message type in pbft proto: %u",
             replica_msg.msg_case());
  }
}

void PbftReplica::HandleRequest(const TransportAddress &remote,
                                const RequestMessage &msg) {
  if (!security.ClientVerifier().Verify(msg.req().SerializeAsString(),
                                        msg.sig())) {
    RWarning("Wrong signature for client");
    return;
  }

  if (!msg.relayed()) {
    clientAddressTable[msg.req().clientid()] =
        unique_ptr<TransportAddress>(remote.clone());
  }
  auto kv = clientTable.find(msg.req().clientid());
  if (kv != clientTable.end()) {
    ClientTableEntry &entry = kv->second;
    if (msg.req().clientreqid() < entry.lastReqId) {
      RNotice("Ignoring stale request");
      return;
    }
    if (msg.req().clientreqid() == entry.lastReqId) {
      RNotice("Received duplicate request; resending reply");
      Assert(clientAddressTable.count(msg.req().clientid()));
      if (!(transport->SendMessage(this,
                                   *clientAddressTable[msg.req().clientid()],
                                   PBMessage(entry.reply)))) {
        RWarning("Failed to resend reply to client");
      }
      return;
    }
  }

  if (!AmPrimary()) {
    RWarning("Received Request but not primary; is primary dead?");
    ToReplicaMessage m;
    *m.mutable_request() = msg;
    m.mutable_request()->set_relayed(true);
    transport->SendMessageToReplica(this, configuration.GetLeaderIndex(view),
                                    PBMessage(m));
    viewChangeTimeout->Start();
    return;
  }

  for (auto &pp : pendingPrePrepareList) {
    if (pp.clientId == msg.req().clientid() &&
        pp.clientReqId == msg.req().clientreqid()) {
      RNotice("Skip propose; active propose exist");
      return;
    }
  }

  seqNum += 1;
  RDebug(PROTOCOL_FMT ", ASSIGNED TO req = %lu@%lu", "preprepare", view, seqNum,
         msg.req().clientreqid(), msg.req().clientid());
  ToReplicaMessage m;
  PrePrepareMessage &prePrepare = *m.mutable_pre_prepare();
  prePrepare.mutable_common()->set_view(view);
  prePrepare.mutable_common()->set_seqnum(seqNum);
  prePrepare.mutable_common()->set_digest(std::string());  // TODO
  security.ReplicaSigner(ReplicaId())
      .Sign(prePrepare.common().SerializeAsString(), *prePrepare.mutable_sig());
  *prePrepare.mutable_message() = msg;
  Assert(prePrepare.common().seqnum() == log.LastOpnum() + 1);
  EnterPrepareRound(prePrepare);
  transport->SendMessageToAll(this, PBMessage(m));

  PendingPrePrepare pp;
  pp.seqNum = seqNum;
  pp.clientId = msg.req().clientid();
  pp.clientReqId = msg.req().clientreqid();
  pp.timeout =
      std::unique_ptr<Timeout>(new Timeout(transport, 300, [this, m = m]() {
        RWarning("Resend PrePrepare seq = %lu",
                 m.pre_prepare().common().seqnum());
        ToReplicaMessage copy(m);
        transport->SendMessageToAll(this, PBMessage(copy));
      }));
  pp.timeout->Start();
  pendingPrePrepareList.push_back(std::move(pp));

  TryEnterCommitRound(prePrepare.common());  // for single replica setup
}

void PbftReplica::HandlePrePrepare(const TransportAddress &remote,
                                   const proto::PrePrepareMessage &msg) {
  if (AmPrimary()) {
    RNotice("Unexpected PrePrepare sent to primary");
    return;
  }

  if (view != msg.common().view()) return;

  if (!security.ReplicaVerifier(configuration.GetLeaderIndex(view))
           .Verify(msg.common().SerializeAsString(), msg.sig())) {
    RWarning("Wrong signature for PrePrepare");
    return;
  }
  // uint64_t clientid = msg.message().req().clientid();
  // if (!clientAddressTable.count(clientid)) {
  //   RWarning("sig@PrePrepare: no client address record");
  //   return;
  // }
  if (!security.ClientVerifier().Verify(msg.message().req().SerializeAsString(),
                                        msg.message().sig())) {
    RWarning("Wrong signature for client in PrePrepare");
    return;
  }

  opnum_t seqNum = msg.common().seqnum();
  // if (commonTable.count(seqNum) && !Match(commonTable[seqNum], msg.common()))
  if (commonTable.count(seqNum)) return;

  // no impl high and low water mark, along with GC

  viewChangeTimeout->Stop();  // TODO filter out faulty message

  if (msg.common().seqnum() > log.LastOpnum() + 1) {
    RWarning("Gap detected; fill with EMPTY and schedule state transfer");
    for (opnum_t seqNum = log.LastOpnum() + 1; seqNum < msg.common().seqnum();
         seqNum += 1) {
      log.Append(
          new LogEntry(viewstamp_t(view, seqNum), LOG_STATE_EMPTY, Request()));
      ScheduleStateTransfer(seqNum);
    }
  }
  Assert(msg.common().seqnum() <= log.LastOpnum() + 1);
  RDebug(PROTOCOL_FMT, "prepare", view, seqNum);
  EnterPrepareRound(msg);

  CommonSend<PrepareMessage>(msg.common(), nullptr);
  ScheduleStateTransfer(msg.common().seqnum());

  prepareSet.Add(seqNum, ReplicaId(), msg.common());
  TryEnterCommitRound(msg.common());
}

void PbftReplica::HandlePrepare(const TransportAddress &remote,
                                const proto::PrepareMessage &msg) {
  if (!security.ReplicaVerifier(msg.replicaid())
           .Verify(msg.common().SerializeAsString(), msg.sig())) {
    RWarning("Wrong signature for Prepare");
    return;
  }

  // TODO verify incoming prepare matches prepared proposal
  if (LoggedPrepared(msg.common().seqnum())) {
    RDebug("not broadcast for delayed Prepare; directly resp instead");
    CommonSend<CommitMessage>(msg.common(), &remote);
    return;
  }

  prepareSet.Add(msg.common().seqnum(), msg.replicaid(), msg.common());
  TryEnterCommitRound(msg.common());
}

void PbftReplica::HandleCommit(const TransportAddress &remote,
                               const proto::CommitMessage &msg) {
  if (!security.ReplicaVerifier(msg.replicaid())
           .Verify(msg.common().SerializeAsString(), msg.sig())) {
    RWarning("Wrong signature for Commit");
    return;
  }

  if (msg.replicaid() == configuration.GetLeaderIndex(view))
    viewChangeTimeout->Stop();  // TODO filter out faulty message

  commitSet.Add(msg.common().seqnum(), msg.replicaid(), msg.common());
  TryReachCommitPoint(msg.common());
}

void PbftReplica::HandleStateTransferRequest(
    const TransportAddress &remote,
    const proto::StateTransferRequestMessage &msg) {
  // TODO view change along with state transfer
  if (commonTable.count(msg.seqnum())) {
    if (AmPrimary()) {
      RNotice("Send PrePrepare on state transfer demand");
      ToReplicaMessage m;
      PrePrepareMessage &prePrepare = *m.mutable_pre_prepare();
      *prePrepare.mutable_common() = commonTable[msg.seqnum()];
      security.ReplicaSigner(ReplicaId())
          .Sign(prePrepare.common().SerializeAsString(),
                *prePrepare.mutable_sig());
      auto *entry = static_cast<LogEntry *>(log.Find(msg.seqnum()));
      *prePrepare.mutable_message()->mutable_req() = entry->request;
      prePrepare.mutable_message()->set_sig(entry->signature);
      prePrepare.mutable_message()->set_relayed(false);  // ok?
      transport->SendMessage(this, remote, PBMessage(m));
    } else {
      RNotice("Send Prepare on state transfer demand");
      CommonSend<PrepareMessage>(commonTable[msg.seqnum()], &remote);
    }
  }
  if (LoggedPrepared(msg.seqnum())) {
    RNotice("Send Commit on state transfer demand");
    CommonSend<CommitMessage>(commonTable[msg.seqnum()], &remote);
  }
};

void PbftReplica::OnViewChange() { NOT_IMPLEMENTED(); }

void PbftReplica::EnterPrepareRound(proto::PrePrepareMessage msg) {
  commonTable[msg.common().seqnum()] = msg.common();
  if (log.LastOpnum() < msg.common().seqnum()) {
    log.Append(new LogEntry(
        viewstamp_t(msg.common().view(), msg.common().seqnum()),
        LOG_STATE_PREPREPARED, msg.message().req(), msg.message().sig()));
    return;
  }

  auto *entry = static_cast<LogEntry *>(log.Find(msg.common().seqnum()));
  if (entry->state != LOG_STATE_EMPTY) return;

  RNotice("PrePrepare gap at seq = %lu is filled", msg.common().seqnum());
  log.SetRequest(msg.common().seqnum(), msg.message().req());
  log.SetStatus(msg.common().seqnum(), LOG_STATE_PREPREPARED);
}

void PbftReplica::TryEnterCommitRound(const proto::Common &msg) {
  Assert(!LoggedPrepared(msg.seqnum()));

  if (!Prepared(msg.seqnum(), msg)) return;

  RDebug(PROTOCOL_FMT, "commit", msg.view(), msg.seqnum());
  log.Find(msg.seqnum())->state = LOG_STATE_PREPARED;
  TrySpeculative();

  if (AmPrimary()) {
    for (auto iter = pendingPrePrepareList.begin();
         iter != pendingPrePrepareList.end(); ++iter) {
      if (iter->seqNum == msg.seqnum()) {
        iter->timeout->Stop();
        pendingPrePrepareList.erase(iter);
        break;
      }
    }
  }

  CommonSend<CommitMessage>(msg, nullptr);
  ScheduleStateTransfer(msg.seqnum());

  commitSet.Add(msg.seqnum(), ReplicaId(), msg);
  TryReachCommitPoint(msg);  // for single replica setup
}

void PbftReplica::TryReachCommitPoint(const proto::Common &msg) {
  Assert(msg.seqnum() <= log.LastOpnum());
  if (log.Find(msg.seqnum())->state == LOG_STATE_COMMITTED) return;

  if (!CommittedLocal(msg.seqnum(), msg)) return;

  RDebug(PROTOCOL_FMT, "commit point", msg.view(), msg.seqnum());
  log.SetStatus(msg.seqnum(), LOG_STATE_COMMITTED);

  for (auto iter = pendingProposalList.begin();
       iter != pendingProposalList.end(); ++iter) {
    if (iter->seqNum == msg.seqnum()) {
      iter->timeout->Stop();
      pendingProposalList.erase(iter);
      break;
    }
  }

  opnum_t executing = msg.seqnum();
  if (lastExecuted != executing - 1) return;
  while (auto *entry = static_cast<LogEntry *>(log.Find(executing))) {
    // speculative case
    if (entry->state == LOG_STATE_SPECULATIVE) {
      Assert(clientTable.count(entry->request.clientid()));
      Assert(clientTable[entry->request.clientid()].lastReqId ==
             entry->request.clientreqid());

      entry->state = LOG_STATE_COMMITTED;
      ToClientMessage m = clientTable[entry->request.clientid()].reply;
      m.mutable_reply()->set_speculative(false);
      transport->SendMessage(
          this, *clientAddressTable[entry->request.clientid()], PBMessage(m));
      executing += 1;
      continue;
    }
    // speculative case end

    if (entry->state != LOG_STATE_COMMITTED) break;
    entry->state = LOG_STATE_COMMITTED;
    ExecuteEntry(entry, false);
    executing += 1;
  }
  lastExecuted = executing - 1;
  TrySpeculative();
}

void PbftReplica::TrySpeculative() {
  opnum_t executing = lastExecuted + 1;
  while (auto *entry = static_cast<LogEntry *>(log.Find(executing))) {
    Assert(entry->state != LOG_STATE_SPECULATIVE);
    Assert(entry->state != LOG_STATE_COMMITTED);
    if (entry->state != LOG_STATE_PREPARED) break;
    RDebug(PROTOCOL_FMT, "spec execution", entry->viewstamp.view,
           entry->viewstamp.opnum);
    entry->state = LOG_STATE_SPECULATIVE;
    ExecuteEntry(entry, true);
    executing += 1;
  }
  lastExecuted = executing - 1;
}

void PbftReplica::ExecuteEntry(LogEntry *entry, bool speculative) {
  const Request &req = entry->request;
  opnum_t executing = entry->viewstamp.opnum;
  if (clientTable.count(req.clientid()) &&
      clientTable[req.clientid()].lastReqId >= req.clientreqid()) {
    RNotice("Skip execute duplicated; seq = %lu, req = %lu@%lu", executing,
            req.clientreqid(), req.clientid());
    if (clientTable[req.clientid()].lastReqId == req.clientreqid()) {
      Assert(clientAddressTable.count(req.clientid()));
      transport->SendMessage(this, *clientAddressTable[req.clientid()],
                             PBMessage(clientTable[req.clientid()].reply));
    }
    return;
  }

  ToClientMessage m;
  proto::ReplyMessage &reply = *m.mutable_reply();
  UpcallArg arg;
  arg.isLeader = AmPrimary();
  Execute(executing, log.Find(executing)->request, reply, &arg);
  reply.set_view(view);
  *reply.mutable_req() = req;
  reply.set_replicaid(ReplicaId());
  reply.set_speculative(speculative);
  reply.set_sig(std::string());
  security.ReplicaSigner(ReplicaId())
      .Sign(reply.SerializeAsString(), *reply.mutable_sig());
  UpdateClientTable(req, m);
  if (clientAddressTable.count(req.clientid()))
    transport->SendMessage(this, *clientAddressTable[req.clientid()],
                           PBMessage(m));
}

void PbftReplica::ScheduleStateTransfer(opnum_t seqNum) {
  for (auto &pp : pendingProposalList) {
    if (pp.seqNum == seqNum) {
      pp.timeout->Reset();
      return;
    }
  }
  PendingProposal pp;
  pp.seqNum = seqNum;
  pp.timeout = unique_ptr<Timeout>(
      new Timeout(transport, 1000, [this, seqNum = seqNum]() {
        Assert(log.Find(seqNum)->state != LOG_STATE_COMMITTED);
        RWarning("State transfer triggered, seq = %lu", seqNum);
        ToReplicaMessage m;
        StateTransferRequestMessage &stReq =
            *m.mutable_state_transfer_request();
        stReq.set_seqnum(seqNum);
        transport->SendMessageToAll(this, PBMessage(m));
      }));
  pp.timeout->Start();
  pendingProposalList.push_back(std::move(pp));
}

void PbftReplica::UpdateClientTable(const Request &req,
                                    const ToClientMessage &reply) {
  ClientTableEntry &entry = clientTable[req.clientid()];
  ASSERT(entry.lastReqId <= req.clientreqid());

  if (entry.lastReqId == req.clientreqid()) {  // Duplicate request
    return;
  }
  entry.lastReqId = req.clientreqid();
  entry.reply = reply;
}

}  // namespace pbft
}  // namespace dsnet
