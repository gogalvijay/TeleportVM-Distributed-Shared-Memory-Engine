#ifndef COORDINATOR_SEQUENCER_H
#define COORDINATOR_SEQUENCER_H

#include "network.h"
#include "gpd.h"
#include <pthread.h>
#include <stdbool.h>

typedef struct deferred_request {
    uint32_t node_id;
    uint8_t type;               
    int client_fd;
    uint64_t page_index;
    struct deferred_request *next;
} deferred_request_t;

typedef struct {
    pthread_mutex_t page_lock;  
    bool transaction_active;    
    uint32_t active_node;      
    uint32_t expected_acks;     
    
    deferred_request_t *queue_head;
    deferred_request_t *queue_tail;
} page_sequencer_t;

void coordinator_sequencer_init(size_t total_pages);
void coordinator_sequencer_destroy(void);
void process_node_request(int client_fd, struct dsm_packet *packet, int *node_fds);
void process_node_invalidate_ack(uint64_t page_index, uint32_t ack_node_id, int *node_fds);

#endif 
