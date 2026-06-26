#ifndef NETWORK_H
#define NETWORK_H

#include <stdint.h>

#define MSG_REQUEST_PAGE_READ  1  
#define MSG_REQUEST_PAGE_WRITE 2
#define MSG_SEND_PAGE          3
#define MSG_NODE_JOIN          4
#define MSG_INVALIDATE         5
#define MSG_INVALIDATE_ACK     6

struct __attribute__((packed)) dsm_packet {
    uint8_t type;
    uint64_t page_index;
    uint32_t sender_node_id;
    uint8_t payload[4096];
};

_Static_assert(sizeof(struct dsm_packet) == 4109, "Packet size mismatch");

#endif
