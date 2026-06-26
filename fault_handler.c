#include "fault_handler.h"
#include "dsm_core.h"
#include "queue.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <linux/userfaultfd.h>

void* fault_handler(void* arg) {
    int uffd = (int)(uintptr_t)arg; 
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 failed");
        return NULL;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = uffd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, uffd, &ev);

    struct epoll_event event[10];

    while (1) {
        int ready_events = epoll_wait(epoll_fd, event, 10, -1);
        for (int i = 0; i < ready_events; i++) {
            if (event[i].data.fd == uffd) {
                struct uffd_msg msg;
                ssize_t msg_bytes = read(uffd, &msg, sizeof(msg));
                if (msg_bytes <= 0) {
                    break;
                }

                if (msg.event != UFFD_EVENT_PAGEFAULT) {
                    break;
                }

                uint64_t fault_address = msg.arg.pagefault.address;
                uint64_t page_base = (fault_address & ~(4096 - 1));
                
                int idx = get_page_index(page_base);
                if (idx == -1) {
                    break;
                }
                
                int is_write = (msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WRITE) ? 1 : 0;
                pthread_mutex_lock(&page_table[idx].lock);

                if (page_table[idx].status == PAGE_VALID && !is_write) {
                    pthread_mutex_unlock(&page_table[idx].lock);
                    continue;
                }

                if (page_table[idx].status == PAGE_VALID && is_write) {
                    page_table[idx].status = 3; 
                } else {
                    page_table[idx].status = PAGE_PENDING;
                }

                page_request_t req;
                req.page_index = (uint64_t)idx;
                req.is_write = is_write;
                if (queue_push(&shared_req_queue, req) < 0) {
                    page_table[idx].status = PAGE_EMPTY;
                }
                
                pthread_mutex_unlock(&page_table[idx].lock);
            }
        }
    }
    return NULL;
}
