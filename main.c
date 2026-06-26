#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <time.h>
#include <pthread.h>

#include "dsm_core.h"
#include "queue.h"
#include "fault_handler.h"
#include "network_engine.h"
#include "gpd.h"

void execute_cluster_benchmark(void *dsm_shared_region, size_t region_size);
void run_coherence_test_loop(void *region, size_t region_size);

extern int coordinator_fd;

void* background_eviction_thread(void* arg) {
    int net_fd = (int)(uintptr_t)arg;
    while (1) {
        usleep(200000);   
        if (net_fd != -1) {
            dsm_execute_eviction(net_fd);
        }
    }
    return NULL;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
        "Usage:\n"
        "  Coordinator (first node, holds the GPD):\n"
        "    %s <node_id> <listen_port> [--coherence-test]\n"
        "  Worker node (connects out to the coordinator):\n"
        "    %s <node_id> <listen_port> <coordinator_host> <coordinator_port> [--coherence-test]\n"
        "\n"
        "Notes:\n"
        "  - node_id must be >= 1 (0 is reserved internally for 'unowned').\n"
        "  - Each node needs its own listen_port if running on one machine.\n"
        "  - Example (2 nodes on localhost):\n"
        "      %s 1 3490\n"
        "      %s 2 3491 127.0.0.1 3490\n"
        "  - With --coherence-test, the normal benchmark is skipped and the\n"
        "    node instead reads WRITE/READ/SLEEP/DONE commands from stdin,\n"
        "    for scripted cross-node conflict testing.\n",
        prog, prog, prog, prog);
}

int main(int argc, char **argv) {
    int test_mode = 0;
    int real_argc = argc;

    if (real_argc > 1 && strcmp(argv[real_argc - 1], "--coherence-test") == 0) {
        test_mode = 1;
        real_argc--;
    }

    if (real_argc != 3 && real_argc != 5) {
        print_usage(argv[0]);
        return 1;
    }

    uint32_t node_id = (uint32_t)strtoul(argv[1], NULL, 10);
    const char *listen_port = argv[2];
    const char *coord_host = (real_argc == 5) ? argv[3] : NULL;
    const char *coord_port = (real_argc == 5) ? argv[4] : NULL;

    if (node_id == 0 || node_id >= 64) {
        fprintf(stderr, "Error: node_id must be between 1 and 63.\n");
        return 1;
    }

    network_set_local_node_id(node_id);
    network_set_listen_port(listen_port);
    if (coord_host && coord_port) {
        network_set_coordinator(coord_host, coord_port);
    }

    page_size = sysconf(_SC_PAGESIZE);
    queue_init(&shared_req_queue);

    size_t total_pages = REGION_SIZE / page_size;
    gpd_init(total_pages);

    dsm_set_eviction_node_id(node_id);

    int uffd = create_user_fault_fd();
    if (uffd < 1) return 1;

    void* region = mmap(NULL, REGION_SIZE, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (region == MAP_FAILED) {
        perror("mmap error");
        return 1;
    }
    region_start = region;
    if (coord_host) {
        printf("[Node %u] listening on port %s | coordinator=%s:%s\n",
               node_id, listen_port, coord_host, coord_port);
    } else {
        printf("[Node %u] listening on port %s (coordinator/home node)\n",
               node_id, listen_port);
    }
    printf("mmap region allocated at %p (%zu bytes)\n", region, (size_t)REGION_SIZE);

    num_pages = total_pages;
    page_table = malloc(num_pages * sizeof(page_tracker_t));
    if (!page_table) {
        perror("Failed to allocate local page table");
        return 1;
    }

    for (size_t i = 0; i < num_pages; i++) {
        page_table[i].base_addr = (uintptr_t)region + (i * page_size);
        page_table[i].status = PAGE_EMPTY;
        pthread_mutex_init(&page_table[i].lock, NULL);
    }
    printf("Local page tracking table initialized with %zu entries.\n", num_pages);

   
    local_page_tracker_t *eviction_ring = malloc(num_pages * sizeof(local_page_tracker_t));
    if (!eviction_ring) {
        perror("Failed to allocate eviction ring");
        return 1;
    }
    for (size_t i = 0; i < num_pages; i++) {
        eviction_ring[i].base_addr   = page_table[i].base_addr;
        eviction_ring[i].page_index  = i;
        eviction_ring[i].status      = 1;
        eviction_ring[i].access_bit  = 0;
        pthread_mutex_init(&eviction_ring[i].page_mutex, NULL);
    }
    dsm_init_eviction_ring(eviction_ring, num_pages);

    register_memory_region(uffd, region);

    pthread_t thread_id;
    int thread_create = pthread_create(&thread_id, NULL, fault_handler, (void*)(uintptr_t)uffd);
    if (thread_create != 0) {
        perror("Thread creation failed");
        return 1;
    }

    pthread_t net_thread;
    int net_create = pthread_create(&net_thread, NULL, network_engine, (void*)(uintptr_t)uffd);
    if (net_create != 0) {
        perror("Network engine thread creation failed");
        return 1;
    }

    struct timespec ts = {0, 500000000};
    nanosleep(&ts, NULL);

    pthread_t evict_thread;
    pthread_create(&evict_thread, NULL, background_eviction_thread,
                   (void*)(uintptr_t)coordinator_fd);

    if (test_mode) {
        run_coherence_test_loop(region, REGION_SIZE);
    } else {
        execute_cluster_benchmark(region, REGION_SIZE);
    }

    pthread_join(thread_id, NULL);
    pthread_join(net_thread, NULL);

    gpd_destroy();
    return 0;
}
