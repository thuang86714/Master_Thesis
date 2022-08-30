
#ifndef _VR_REPLICA_H_
#define _VR_REPLICA_H_

#include "lib/configuration.h"
#include "lib/latency.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "replication/vr/vr-proto.pb.h"

#include <map>
#include <memory>
#include <list>
namespace dsnet {
namespace vr {
  
void newTimeoutandLatency();
  
uint64_t GenerateNonce() const;
  
bool AmLeader() const;
  
void CommitUpTo(opnum_t upto);  
  
void SendPrepareOKs(opnum_t oldLastOp);
  
void SendRecoveryMessages();//do nothing
  
void RequestStateTransfer();
  
void EnterView(view_t newview);
  
void StartViewChange(view_t newview);
  
void SendNullCommit();//do nothing
  
void UpdateClientTable(const Request &req);
    
void CloseBatch();

void HandleUnloggedRequest(const proto::UnloggedRequestMessage &msg);

void HandlePrepare(const proto::PrepareMessage &msg);

void HandleCommit(const proto::CommitMessage &msg);

void HandleRequestStateTransfer(const proto::RequestStateTransferMessage &msg);

void HandleStateTransfer(const proto::StateTransferMessage &msg);

void HandleStartViewChange(const proto::StartViewChangeMessage &msg);

void HandleDoViewChange(const proto::DoViewChangeMessage &msg);

void HandleStartView(const proto::StartViewMessage &msg);

void HandleRecovery(const proto::RecoveryMessage &msg);

void HandleRecoveryResponse(const proto::RecoveryResponseMessage &msg);
}
}
