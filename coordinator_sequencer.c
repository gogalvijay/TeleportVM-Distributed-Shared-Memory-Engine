#include "coordinator_sequencer.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

static page_sequencer_t *sequencer_table = NULL;
static size_t total_managed_pages = 0;

static void send_packet_to_node(int fd, struct dsm_packet *pkt) {
    size_t total_sent = 0;
    size_t len = sizeof(struct dsm_packet);
    const uint8_t *ptr = (const uint8_t *)pkt;

    while (total_sent < len) {
        ssize_t sent = send(fd, ptr + total_sent, len - total_sent, MSG_DONTWAIT);
        if (sent > 0) {
            total_sent += sent;
        } else {
            break;
        }
    }
}

void coordinator_sequencer_init(size_t total_pages) {
    total_managed_pages = total_pages;
    sequencer_table = malloc(total_pages * sizeof(page_sequencer_t));
    if (!sequencer_table) {
        perror("Failed to allocate Coordinator Sequencer Table");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < total_pages; i++) {
        pthread_mutex_init(&sequencer_table[i].page_lock, NULL);
        sequencer_table[i].transaction_active = false;
        sequencer_table[i].active_node = 0;
        sequencer_table[i].expected_acks = 0;
        sequencer_table[i].queue_head = NULL;
        sequencer_table[i].queue_tail = NULL;
    }
    printf("[Coordinator] Locking Sequencer initialized for %zu pages.\n", total_pages);
}

void coordinator_sequencer_destroy(void) {
    if (!sequencer_table) return;

    for (size_t i = 0; i < total_managed_pages; i++) {
        pthread_mutex_lock(&sequencer_table[i].page_lock);
        deferred_request_t *curr = sequencer_table[i].queue_head;
        while (curr) {
            deferred_request_t *tmp = curr->next;
            free(curr);
            curr = tmp;
        }
        pthread_mutex_unlock(&sequencer_table[i].page_lock);
        pthread_mutex_destroy(&sequencer_table[i].page_lock);
    }
    free(sequencer_table);
}

static void execute_gpd_transaction(page_sequencer_t *seq, uint64_t page_idx, uint32_t node_id, uint8_t type, int client_fd, int *node_fds) {
    seq->transaction_active = true;
    seq->active_node = node_id;

    if (type == MSG_REQUEST_PAGE_WRITE) {
        uint64_t invalidate_mask = 0;
        
        gpd_transition_to_modified(page_idx, node_id, &invalidate_mask);
        
        uint32_t acks_needed = 0;
        for (int i = 0; i < 64; i++) {
            if ((invalidate_mask & (1ULL << i)) && i != (int)node_id) {
                acks_needed++;
            }
        }
        
        seq->expected_acks = acks_needed;

        if (acks_needed > 0) {
            struct dsm_packet inv_packet;
            memset(&inv_packet, 0, sizeof(inv_packet));
            inv_packet.type = MSG_INVALIDATE;
            inv_packet.page_index = page_idx;
            inv_packet.sender_node_id = 0; 

            for (int i = 0; i < 64; i++) {
                if ((invalidate_mask & (1ULL << i)) && i != (int)node_id) {
                    if (node_fds[i] != -1) {
                        send_packet_to_node(node_fds[i], &inv_packet);
                    }
                }
            }
            printf("[Coordinator] Page %lu locked. Dispatched %u Invalidation broadcasts.\n", page_idx, acks_needed);
        } else {
            struct dsm_packet grant_packet;
            memset(&grant_packet, 0, sizeof(grant_packet));
            grant_packet.type = MSG_SEND_PAGE; 
            grant_packet.page_index = page_idx;
            grant_packet.sender_node_id = 0;
            
            send_packet_to_node(client_fd, &grant_packet);
            
            seq->transaction_active = false;
            seq->active_node = 0;
            printf("[Coordinator] Page %lu immediately granted WRITE to Node %u.\n", page_idx, node_id);
        }
        
    } else if (type == MSG_REQUEST_PAGE_READ) {
        uint32_t old_owner = 0;
        gpd_transition_to_shared(page_idx, node_id, &old_owner);

        if (old_owner != 0 && old_owner != node_id) {
            seq->expected_acks = 1; 
            
            struct dsm_packet fetch_packet;
            memset(&fetch_packet, 0, sizeof(fetch_packet));
            fetch_packet.type = MSG_INVALIDATE; 
            fetch_packet.page_index = page_idx;
            fetch_packet.sender_node_id = node_id; 

            send_packet_to_node(node_fds[old_owner], &fetch_packet);
        } else {
            struct dsm_packet grant_packet;
            memset(&grant_packet, 0, sizeof(grant_packet));
            grant_packet.type = MSG_SEND_PAGE;
            grant_packet.page_index = page_idx;
            
            send_packet_to_node(client_fd, &grant_packet);
            seq->transaction_active = false;
            seq->active_node = 0;
        }
    }
}

void process_node_request(int client_fd, struct dsm_packet *packet, int *node_fds) {
    uint64_t page_idx = packet->page_index;
    uint32_t node_id = packet->sender_node_id;
    uint8_t type = packet->type;

    if (page_idx >= total_managed_pages) return;

    page_sequencer_t *seq = &sequencer_table[page_idx];
    pthread_mutex_lock(&seq->page_lock);

    if (seq->transaction_active) {
        deferred_request_t *new_req = malloc(sizeof(deferred_request_t));
        new_req->node_id = node_id;
        new_req->type = type;
        new_req->client_fd = client_fd;
        new_req->page_index = page_idx;
        new_req->next = NULL;

        if (!seq->queue_head) {
            seq->queue_head = new_req;
            seq->queue_tail = new_req;
        } else {
            seq->queue_tail->next = new_req;
            seq->queue_tail = new_req;
        }
        printf("[Coordinator] Race Prevented: Queued %s request from Node %u for Page %lu\n",
               (type == MSG_REQUEST_PAGE_WRITE) ? "WRITE" : "READ", node_id, page_idx);
        
        pthread_mutex_unlock(&seq->page_lock);
        return;
    }

    execute_gpd_transaction(seq, page_idx, node_id, type, client_fd, node_fds);
    pthread_mutex_unlock(&seq->page_lock);
}

void process_node_invalidate_ack(uint64_t page_index, uint32_t ack_node_id, int *node_fds) {
    if (page_index >= total_managed_pages) return;

    page_sequencer_t *seq = &sequencer_table[page_index];
    pthread_mutex_lock(&seq->page_lock);

    if (!seq->transaction_active || seq->expected_acks == 0) {
        pthread_mutex_unlock(&seq->page_lock);
        return;
    }

    seq->expected_acks--;
    printf("[Coordinator] Received Invalidate ACK from Node %u for Page %lu. Remaining: %u\n",
           ack_node_id, page_index, seq->expected_acks);

    if (seq->expected_acks == 0) {
        struct dsm_packet grant_packet;
        memset(&grant_packet, 0, sizeof(grant_packet));
        grant_packet.type = MSG_SEND_PAGE;
        grant_packet.page_index = page_index;
        
        int active_node_fd = node_fds[seq->active_node];
        if (active_node_fd != -1) {
            send_packet_to_node(active_node_fd, &grant_packet);
        }

        printf("[Coordinator] Transaction Complete. Page %lu ownership officially shifted to Node %u.\n", 
               page_index, seq->active_node);

        seq->transaction_active = false;
        seq->active_node = 0;

        if (seq->queue_head != NULL) {
            deferred_request_t *next_req = seq->queue_head;
            seq->queue_head = next_req->next;
            if (!seq->queue_head) {
                seq->queue_tail = NULL;
            }

            printf("[Coordinator] Processing deferred request from queue for Node %u (Page %lu)\n", 
                   next_req->node_id, next_req->page_index);
            
            execute_gpd_transaction(seq, next_req->page_index, next_req->node_id, next_req->type, next_req->client_fd, node_fds);
            
            free(next_req);
        }
    }

    pthread_mutex_unlock(&seq->page_lock);
}
