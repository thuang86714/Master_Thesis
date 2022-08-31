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

/* This function prepares client side connection resources for an RDMA connection */
int client_prepare_connection(struct sockaddr_in *s_addr);
		  
int client_pre_post_recv_buffer();

/* Connects to the RDMA server */
int client_connect_to_server();

/* Exchange buffer metadata with the server. The client sends its, and then receives
 * from the server. The client-side metadata on the server is _not_ used because
 * this program is client driven. But it shown here how to do it for the illustration
 * purposes
 */
int client_xchange_metadata_with_server();
	
int client_disconnect_and_clean();
}
}
