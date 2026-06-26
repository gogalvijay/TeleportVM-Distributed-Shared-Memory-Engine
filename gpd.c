#include "gpd.h"
#include <stdlib.h>
#include <stdio.h>

gpd_entry_t *global_page_directory = NULL;
size_t gpd_num_pages = 0;

void gpd_init(size_t total_pages) {
    gpd_num_pages = total_pages;
    global_page_directory = malloc(gpd_num_pages * sizeof(gpd_entry_t));
    if (!global_page_directory) {
        perror("Failed to allocate GPD");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < gpd_num_pages; i++) {
        atomic_init(&global_page_directory[i].state, STATE_INVALID);
        atomic_init(&global_page_directory[i].owner_node_id, 0);
        atomic_init(&global_page_directory[i].shared_nodes_bitmap, 0);
        pthread_mutex_init(&global_page_directory[i].transaction_lock, NULL);
    }
}

void gpd_destroy(void) {
    if (global_page_directory) {
        for (size_t i = 0; i < gpd_num_pages; i++) {
            pthread_mutex_destroy(&global_page_directory[i].transaction_lock);
        }
        free(global_page_directory);
    }
}

msi_state_t gpd_get_state(size_t page_index) {
    return atomic_load(&global_page_directory[page_index].state);
}

uint32_t gpd_get_owner(size_t page_index) {
    return atomic_load(&global_page_directory[page_index].owner_node_id);
}

uint64_t gpd_get_shared_mask(size_t page_index) {
    return atomic_load(&global_page_directory[page_index].shared_nodes_bitmap);
}

static const char *msi_name(msi_state_t s) {
    switch (s) {
        case STATE_MODIFIED: return "MODIFIED";
        case STATE_SHARED:   return "SHARED";
        case STATE_INVALID:  return "INVALID";
        default:             return "?";
    }
}

bool gpd_transition_to_shared(size_t page_index, uint32_t reading_node, uint32_t *out_old_owner) {
    if (page_index >= gpd_num_pages) return false;

    gpd_entry_t *entry = &global_page_directory[page_index];
    pthread_mutex_lock(&entry->transaction_lock);

    msi_state_t current_state = atomic_load(&entry->state);
    *out_old_owner = atomic_load(&entry->owner_node_id);

    printf("[GPD] page=%zu %s -> SHARED (reader=node%u, prev_owner=node%u)\n",
           page_index, msi_name(current_state), reading_node, *out_old_owner);

    if (current_state == STATE_MODIFIED) {
        atomic_store(&entry->state, STATE_SHARED);
        
        atomic_store(&entry->owner_node_id, 0);
        uint64_t mask = (1ULL << *out_old_owner) | (1ULL << reading_node);
        atomic_store(&entry->shared_nodes_bitmap, mask);
    } else {
        atomic_store(&entry->state, STATE_SHARED);
        atomic_store(&entry->owner_node_id, 0);
        uint64_t current_mask = atomic_load(&entry->shared_nodes_bitmap);
        atomic_store(&entry->shared_nodes_bitmap, current_mask | (1ULL << reading_node));
    }

    pthread_mutex_unlock(&entry->transaction_lock);
    return true;
}

bool gpd_transition_to_modified(size_t page_index, uint32_t writing_node, uint64_t *out_invalidate_mask) {
    if (page_index >= gpd_num_pages) return false;

    gpd_entry_t *entry = &global_page_directory[page_index];
    pthread_mutex_lock(&entry->transaction_lock);

    msi_state_t current_state = atomic_load(&entry->state);
    uint64_t holders_mask = 0;

    if (current_state == STATE_MODIFIED) {
        uint32_t prev_owner = atomic_load(&entry->owner_node_id);
        if (prev_owner != 0) {
            holders_mask = (1ULL << prev_owner);
        }
    } else if (current_state == STATE_SHARED) {
        holders_mask = atomic_load(&entry->shared_nodes_bitmap);
    }

    *out_invalidate_mask = holders_mask & ~(1ULL << writing_node);

    printf("[GPD] page=%zu %s -> MODIFIED (writer=node%u, invalidating_mask=0x%lx)\n",
           page_index, msi_name(current_state), writing_node,
           (unsigned long)*out_invalidate_mask);

    atomic_store(&entry->state, STATE_MODIFIED);
    atomic_store(&entry->owner_node_id, writing_node);
    atomic_store(&entry->shared_nodes_bitmap, 0); 

    pthread_mutex_unlock(&entry->transaction_lock);
    return true;
}
