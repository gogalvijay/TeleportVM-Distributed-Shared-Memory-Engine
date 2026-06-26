#ifndef GPD_H
#define GPD_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

typedef enum {
    STATE_MODIFIED,
    STATE_SHARED,
    STATE_INVALID
} msi_state_t;

typedef struct {
    _Atomic msi_state_t state;
    _Atomic uint32_t owner_node_id;
    _Atomic uint64_t shared_nodes_bitmap;
    pthread_mutex_t transaction_lock;
} gpd_entry_t;

extern gpd_entry_t *global_page_directory;
extern size_t gpd_num_pages;

void gpd_init(size_t total_pages);
void gpd_destroy(void);

msi_state_t gpd_get_state(size_t page_index);
uint32_t gpd_get_owner(size_t page_index);
uint64_t gpd_get_shared_mask(size_t page_index);

bool gpd_transition_to_shared(size_t page_index, uint32_t reading_node, uint32_t *out_old_owner);
bool gpd_transition_to_modified(size_t page_index, uint32_t writing_node, uint64_t *out_invalidate_mask);

#endif
