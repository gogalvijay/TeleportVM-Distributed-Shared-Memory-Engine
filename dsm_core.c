#include "dsm_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/userfaultfd.h>

long page_size = 4096;
void* region_start = NULL;
page_tracker_t *page_table = NULL;
size_t num_pages = 0;
request_queue_t shared_req_queue;

int get_page_index(uint64_t page_base) {
    if ((void*)page_base < region_start) return -1;
    size_t offset = page_base - (uintptr_t)region_start;
    size_t idx = offset / page_size;
    return (idx < num_pages) ? (int)idx : -1;
}

int create_user_fault_fd(void) {
    int uffd = syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK);
    if (uffd < 0) {
        perror("userfd syscall error");
        return -1;
    }

    struct uffdio_api api;
    api.api = UFFD_API;
    api.features = 0;
    
    if (ioctl(uffd, UFFDIO_API, &api) < 0) {
        perror("ioctl userfd error");
        close(uffd);
        return -1;
    }

    return uffd;
}

void register_memory_region(int uffd, void *region) {
    struct uffdio_register register_region;
    
    register_region.range.start = (unsigned long)region;
    register_region.range.len = REGION_SIZE;
    register_region.mode = UFFDIO_REGISTER_MODE_MISSING | UFFDIO_REGISTER_MODE_WP;

    if (ioctl(uffd, UFFDIO_REGISTER, &register_region) < 0) {
        perror("register_memory_error");
        exit(EXIT_FAILURE);
    }
}
