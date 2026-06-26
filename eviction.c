#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include "gpd.h"
#include "network.h"
#include "dsm_core.h"


local_page_tracker_t *global_eviction_ring = NULL;
size_t global_tracked_page_count = 0;
static size_t clock_hand = 0;
static pthread_mutex_t clock_hand_mutex = PTHREAD_MUTEX_INITIALIZER;

static uint32_t eviction_node_id = 0;

void dsm_set_eviction_node_id(uint32_t id) {
    eviction_node_id = id;
}

uint32_t get_node_id(void) {
    return eviction_node_id;
}

msi_state_t get_local_msi_state(uint64_t idx) {
    if (idx >= gpd_num_pages) return STATE_INVALID;

    msi_state_t global_state = gpd_get_state((size_t)idx);
    if (global_state == STATE_INVALID) return STATE_INVALID;

    if (global_state == STATE_MODIFIED) {
        uint32_t owner = gpd_get_owner((size_t)idx);
        return (owner == eviction_node_id) ? STATE_MODIFIED : STATE_INVALID;
    }

    uint64_t mask = gpd_get_shared_mask((size_t)idx);
    return (mask & (1ULL << eviction_node_id)) ? STATE_SHARED : STATE_INVALID;
}

void set_local_msi_state(uint64_t idx, msi_state_t st) {
    if (idx >= gpd_num_pages) return;

    gpd_entry_t *entry = &global_page_directory[idx];
    pthread_mutex_lock(&entry->transaction_lock);

    atomic_store(&entry->state, st);

    if (st == STATE_INVALID) {
        uint64_t mask = atomic_load(&entry->shared_nodes_bitmap);
        mask &= ~(1ULL << eviction_node_id);
        atomic_store(&entry->shared_nodes_bitmap, mask);

        uint32_t owner = atomic_load(&entry->owner_node_id);
        if (owner == eviction_node_id) {
            atomic_store(&entry->owner_node_id, 0);
        }
    }

    pthread_mutex_unlock(&entry->transaction_lock);
}

int send_all_bytes(int fd, const void *buf, size_t len) {
    size_t sent = 0;
    const uint8_t *ptr = (const uint8_t *)buf;
    while (sent < len) {
        ssize_t n = send(fd, ptr + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

int recv_all_bytes(int fd, void *buf, size_t len) {
    size_t recvd = 0;
    uint8_t *ptr = (uint8_t *)buf;
    while (recvd < len) {
        ssize_t n = recv(fd, ptr + recvd, len - recvd, 0);
        if (n <= 0) return -1;
        recvd += (size_t)n;
    }
    return 0;
}

void dsm_mark_accessed(uint64_t page_index) {
    if (page_index < global_tracked_page_count) {
        __atomic_store_n(&global_eviction_ring[page_index].access_bit, 1, __ATOMIC_RELEASE);
    }
}

void dsm_init_eviction_ring(local_page_tracker_t *pages, size_t count) {
    global_eviction_ring = pages;
    global_tracked_page_count = count;
    clock_hand = 0;
}

int dsm_execute_eviction(int network_fd) {
    pthread_mutex_lock(&clock_hand_mutex);

    if (!global_eviction_ring || global_tracked_page_count == 0) {
        pthread_mutex_unlock(&clock_hand_mutex);
        return -1;
    }

    size_t iterations = 0;
    while (iterations < (global_tracked_page_count * 2)) {
        size_t inspect_idx = clock_hand;
        clock_hand = (clock_hand + 1) % global_tracked_page_count;
        iterations++;

        local_page_tracker_t *victim = &global_eviction_ring[inspect_idx];

        if (pthread_mutex_trylock(&victim->page_mutex) != 0) {
            continue;
        }

        uint8_t a_bit = __atomic_load_n(&victim->access_bit, __ATOMIC_ACQUIRE);
        if (a_bit == 1) {
            __atomic_store_n(&victim->access_bit, 0, __ATOMIC_RELEASE);
            pthread_mutex_unlock(&victim->page_mutex);
            continue;
        }

        if (victim->status == 0) {
            pthread_mutex_unlock(&victim->page_mutex);
            continue;
        }

        msi_state_t current_state = get_local_msi_state(victim->page_index);

        if (current_state == STATE_MODIFIED) {
            struct dsm_packet writeback_pkt;
            memset(&writeback_pkt, 0, sizeof(writeback_pkt));
            writeback_pkt.type = MSG_SEND_PAGE;
            writeback_pkt.page_index = victim->page_index;
            writeback_pkt.sender_node_id = get_node_id();
            memcpy(writeback_pkt.payload, (void *)victim->base_addr, 4096);

            if (send_all_bytes(network_fd, &writeback_pkt, sizeof(struct dsm_packet)) == 0) {
                uint8_t ack = 0;
                recv_all_bytes(network_fd, &ack, sizeof(uint8_t));
            }
        }

        madvise((void *)victim->base_addr, 4096, MADV_DONTNEED);
        victim->status = 0;

        set_local_msi_state(victim->page_index, STATE_INVALID);

        pthread_mutex_unlock(&victim->page_mutex);
        pthread_mutex_unlock(&clock_hand_mutex);
        return 0;
    }

    pthread_mutex_unlock(&clock_hand_mutex);
    return -1;  
}
