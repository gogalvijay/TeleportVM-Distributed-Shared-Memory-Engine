#ifndef NETWORK_ENGINE_H
#define NETWORK_ENGINE_H

#include "network.h"
#include <stddef.h>

typedef enum {
    SESSION_LISTENER,
    SESSION_PEER,
    SESSION_QUEUE
} session_type_t;

typedef struct {
    int fd;
    session_type_t type;
    struct dsm_packet packet_buffer;
    size_t bytes_read;
} peer_session_t;

int connect_to_remote_node(int epoll_fd, const char *host, const char *port);
void* network_engine(void* arg);


void network_set_local_node_id(uint32_t id);
void network_set_listen_port(const char *port);
void network_set_coordinator(const char *host, const char *port);

#endif
