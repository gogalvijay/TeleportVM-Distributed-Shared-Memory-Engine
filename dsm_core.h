#ifndef DSM_CORE_H
#define DSM_CORE_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include "queue.h"

#define REGION_SIZE (4 * 4096)

typedef enum {
    PAGE_EMPTY,
    PAGE_PENDING,
    PAGE_VALID
} page_status_t;

typedef struct {
    uintptr_t base_addr;
    page_status_t status;
    pthread_mutex_t lock;
} page_tracker_t;

extern long page_size;
extern void* region_start;
extern page_tracker_t *page_table;
extern size_t num_pages;
extern request_queue_t shared_req_queue;

static inline uint64_t get_page_base(uint64_t fault_address) {
    return fault_address & ~(page_size - 1);
}

int get_page_index(uint64_t page_base);
int create_user_fault_fd(void);
void register_memory_region(int uffd, void *region);


typedef struct {
    uintptr_t base_addr;
    uint64_t page_index;
    uint8_t status;
    uint8_t access_bit;
    pthread_mutex_t page_mutex;
} local_page_tracker_t;

void dsm_init_eviction_ring(local_page_tracker_t *pages, size_t count);
void dsm_mark_accessed(uint64_t page_index);
int dsm_execute_eviction(int network_fd);
void dsm_set_eviction_node_id(uint32_t id);

#endif 
