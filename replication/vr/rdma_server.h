#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>

#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include "rdma_common.h"
namespace dsnet {
namespace vr {

static int setup_client_resources();

/* Starts an RDMA server by allocating basic connection resources */
static int start_rdma_server(struct sockaddr_in *server_addr);

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int accept_client_connection();

/* This function sends server side buffer metadata to the connected client */
static int send_server_metadata_to_client();

/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
static int disconnect_and_cleanup();

void usage();
  
}
}
