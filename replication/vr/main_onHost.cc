#include "common/replica.h"

#include <sched.h>
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "lib/configuration.h"
#include "lib/dpdktransport.h"
#include "lib/signature.h"
#include "lib/udptransport.h"
#include "replication/vr/replica.h"

int main(int argc, char **argv) 
{
  VR = new VRReplica(config,myIdx, true,transport, 1, nullApp);
	//RDMA is ready, do 6 times of rdma Receive to get constructor input
	//VRReplica(agc =6 )
	while(true){
		rdma_server_receive();
	}
	return 0;
}
