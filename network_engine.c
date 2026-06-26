#include "network_engine.h"
#include "dsm_core.h"
#include "queue.h"
#include "gpd.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <linux/userfaultfd.h>
#include <sched.h>
#include <poll.h>

#define MSG_INVALIDATE_ACK 6
#define PAGE_WRITE_PENDING 3  

int coordinator_fd = -1;
static int node_fds[64];
static uint32_t local_node_id = 1;

static char g_listen_port[16] = "3490";
static char g_coord_host[256] = {0};
static char g_coord_port[16]  = {0};
static int  g_have_coordinator = 0;

void network_set_local_node_id(uint32_t id) {
    local_node_id = id;
}

void network_set_listen_port(const char *port) {
    if (port && port[0]) {
        snprintf(g_listen_port, sizeof(g_listen_port), "%s", port);
    }
}

void network_set_coordinator(const char *host, const char *port) {
    if (host && port && host[0] && port[0]) {
        snprintf(g_coord_host, sizeof(g_coord_host), "%s", host);
        snprintf(g_coord_port, sizeof(g_coord_port), "%s", port);
        g_have_coordinator = 1;
    }
}

typedef struct {
    uint32_t requester_node_id;
    uint8_t type;
    int client_fd;
    uint32_t expected_acks;
    uint8_t deferred_payload[4096];
} coord_pending_t;

static coord_pending_t *gpd_pending_reqs = NULL;

static ssize_t send_all_nonblocking(int fd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const uint8_t *raw_ptr = (const uint8_t *)buf;

    while (total_sent < len) {
        ssize_t sent = send(fd, raw_ptr + total_sent, len - total_sent, 0);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd;
                pfd.fd = fd;
                pfd.events = POLLOUT;
                if (poll(&pfd, 1, 100) <= 0) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        total_sent += sent;
    }
    return total_sent;
}

static void invalidate_local_page_copy(uint64_t page_index) {
    if (page_index >= num_pages) return;
    pthread_mutex_lock(&page_table[page_index].lock);
    madvise((void*)page_table[page_index].base_addr, page_size, MADV_DONTNEED);
    page_table[page_index].status = PAGE_EMPTY;
    pthread_mutex_unlock(&page_table[page_index].lock);
}


static void writeprotect_local_page_copy(int uffd, uint64_t page_index) {
    if (page_index >= num_pages) return;
    pthread_mutex_lock(&page_table[page_index].lock);
    if (page_table[page_index].status == PAGE_VALID) {
        struct uffdio_writeprotect wp;
        wp.range.start = (unsigned long)page_table[page_index].base_addr;
        wp.range.len = page_size;
        wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;
        ioctl(uffd, UFFDIO_WRITEPROTECT, &wp);
    }
    pthread_mutex_unlock(&page_table[page_index].lock);
}

int connect_to_remote_node(int epoll_fd, const char *host, const char *port) {
    struct addrinfo hints, *res, *rp;
    int client_fd = -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port, &hints, &res) != 0) {
        return -1;
    }

    for (rp = res; rp != NULL; rp = rp->ai_next) {
        client_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (client_fd == -1) continue;

        fcntl(client_fd, F_SETFL, O_NONBLOCK);

        if (connect(client_fd, rp->ai_addr, rp->ai_addrlen) == -1) {
            if (errno != EINPROGRESS) {
                close(client_fd);
                client_fd = -1;
                continue;
            }
        }
        break;
    }

    freeaddrinfo(res);
    if (client_fd == -1) return -1;

    peer_session_t *session = malloc(sizeof(peer_session_t));
    if (!session) {
        close(client_fd);
        return -1;
    }
    session->fd = client_fd;
    session->type = SESSION_PEER;
    session->bytes_read = 0;
    memset(&session->packet_buffer, 0, sizeof(struct dsm_packet));

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET; 
    ev.data.ptr = session;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
        close(client_fd);
        free(session);
        return -1;
    }

    coordinator_fd = client_fd;

    struct dsm_packet join_pkt;
    join_pkt.type = MSG_NODE_JOIN;
    join_pkt.page_index = 0;
    join_pkt.sender_node_id = local_node_id;
    memset(join_pkt.payload, 0, 4096);
    send_all_nonblocking(client_fd, &join_pkt, sizeof(struct dsm_packet));

    return client_fd;
}

void* network_engine(void* arg) {
    int uffd = (int)(uintptr_t)arg;
    struct addrinfo addr, *res, *it;
    int sock_fd;

    if (!gpd_pending_reqs && num_pages > 0) {
        gpd_pending_reqs = calloc(num_pages, sizeof(coord_pending_t));
    }

    for (uint32_t m = 0; m < 64; m++) {
        node_fds[m] = -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.ai_family = AF_UNSPEC;
    addr.ai_socktype = SOCK_STREAM;
    addr.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, g_listen_port, &addr, &res) != 0) {
        perror("getaddrinfo");
        exit(1);
    }

    for (it = res; it != NULL; it = it->ai_next) {
        sock_fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock_fd != -1) {
            fcntl(sock_fd, F_SETFL, O_NONBLOCK);
            int opt = 1;
            setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            if (bind(sock_fd, it->ai_addr, it->ai_addrlen) == 0)
                break;
            close(sock_fd);
        }
    }

    if (it == NULL) {
        perror("failed to bind");
        exit(1);
    }

    freeaddrinfo(res);

    if (listen(sock_fd, 10) == -1) {
        perror("sock_listen_error");
        exit(1);
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll create error");
        return NULL;
    }

    peer_session_t *listener_session = malloc(sizeof(peer_session_t));
    if (!listener_session) {
        perror("listener context allocation failed");
        exit(1);
    }
    listener_session->fd = sock_fd;
    listener_session->type = SESSION_LISTENER;

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = listener_session;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &ev);

    peer_session_t *queue_session = malloc(sizeof(peer_session_t));
    if (!queue_session) {
        perror("queue context allocation failed");
        exit(1);
    }
    queue_session->fd = shared_req_queue.notify_fd;
    queue_session->type = SESSION_QUEUE;

    struct epoll_event q_ev;
    q_ev.events = EPOLLIN | EPOLLET;
    q_ev.data.ptr = queue_session;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, shared_req_queue.notify_fd, &q_ev);

    
    if (g_have_coordinator) {
        printf("[Node %u] Connecting to coordinator at %s:%s...\n",
               local_node_id, g_coord_host, g_coord_port);
        if (connect_to_remote_node(epoll_fd, g_coord_host, g_coord_port) < 0) {
            fprintf(stderr, "[Node %u] Failed to connect to coordinator %s:%s\n",
                    local_node_id, g_coord_host, g_coord_port);
        }
    }

    struct epoll_event epoll_events[10];

    while (1) {
        int n_events = epoll_wait(epoll_fd, epoll_events, 10, -1);
        for (int i = 0; i < n_events; i++) {
            peer_session_t *current_session = (peer_session_t *)epoll_events[i].data.ptr;

            if (current_session->type == SESSION_LISTENER) {
                while (1) {
                    struct sockaddr client;
                    socklen_t client_len = sizeof(client);
                    int client_sock_fd = accept(sock_fd, &client, &client_len);

                    if (client_sock_fd == -1) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            perror("client connection error");
                        }
                        break;
                    }

                    fcntl(client_sock_fd, F_SETFL, O_NONBLOCK);

                    peer_session_t *session = malloc(sizeof(peer_session_t));
                    if (!session) {
                        close(client_sock_fd);
                        continue;
                    }
                    session->fd = client_sock_fd;
                    session->type = SESSION_PEER;
                    session->bytes_read = 0;
                    memset(&session->packet_buffer, 0, sizeof(struct dsm_packet));

                    struct epoll_event client_epoll;
                    client_epoll.events = EPOLLIN | EPOLLET;
                    client_epoll.data.ptr = session;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_sock_fd, &client_epoll);
                }
            } 
            else if (current_session->type == SESSION_QUEUE) {
                uint64_t finished_faults;
                if (read(current_session->fd, &finished_faults, sizeof(finished_faults)) > 0) {
                    page_request_t out_req;
                    while (queue_pop(&shared_req_queue, &out_req) == 0) {
                        uint64_t idx = out_req.page_index;
                        struct dsm_packet packet;
                        if (out_req.is_write) {
                            packet.type = MSG_REQUEST_PAGE_WRITE;
                        } else {
                            packet.type = MSG_REQUEST_PAGE_READ;
                        }
                        packet.page_index = idx;
                        packet.sender_node_id = local_node_id;
                        memset(packet.payload, 0, 4096);
                        
                        if (coordinator_fd != -1) {
                            send_all_nonblocking(coordinator_fd, &packet, sizeof(struct dsm_packet));
                        } else {
                            uint32_t old_owner = gpd_get_owner(idx);
                            pthread_mutex_lock(&page_table[idx].lock);
                            /*
                             * PAGE_WRITE_PENDING (3) means fault_handler saw PAGE_VALID and
                             * changed status before pushing the request.  The page data IS
                             * present locally — this is a write-upgrade, not a cache miss.
                             * Treat it the same as PAGE_VALID so we never fall into the
                             * "fetch from remote old_owner" branch below.
                             */
                            if (page_table[idx].status == PAGE_VALID ||
                                page_table[idx].status == PAGE_WRITE_PENDING) {
                                if (out_req.is_write) {
                                    uint64_t invalid_mask = 0;
                                    gpd_transition_to_modified(idx, local_node_id, &invalid_mask);
                                    
                                    uint32_t ack_count = 0;
                                    for (uint32_t m = 0; m < 64; m++) {
                                        if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1) {
                                            ack_count++;
                                        }
                                    }

                                    if (ack_count > 0) {
                                        gpd_pending_reqs[idx].requester_node_id = local_node_id;
                                        gpd_pending_reqs[idx].type = MSG_REQUEST_PAGE_WRITE;
                                        gpd_pending_reqs[idx].client_fd = -1;
                                        gpd_pending_reqs[idx].expected_acks = ack_count;
                                        
                                        for (uint32_t m = 0; m < 64; m++) {
                                            if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1) {
                                                struct dsm_packet inv_packet;
                                                inv_packet.type = MSG_INVALIDATE;
                                                inv_packet.page_index = idx;
                                                inv_packet.sender_node_id = 0;
                                                memset(inv_packet.payload, 0, 4096);
                                                send_all_nonblocking(node_fds[m], &inv_packet, sizeof(struct dsm_packet));
                                            }
                                        }
                                    } else {
                                        /* No sharers to invalidate — lift write-protection now */
                                        struct uffdio_writeprotect wp;
                                        wp.range.start = (unsigned long)page_table[idx].base_addr;
                                        wp.range.len = page_size;
                                        wp.mode = 0;
                                        ioctl(uffd, UFFDIO_WRITEPROTECT, &wp);
                                        page_table[idx].status = PAGE_VALID;
                                    }
                                }
                                pthread_mutex_unlock(&page_table[idx].lock);
                            } else {
                                pthread_mutex_unlock(&page_table[idx].lock);
                                if (old_owner != 0 && old_owner < 64 && node_fds[old_owner] != -1) {
                                    if (gpd_pending_reqs) {
                                        gpd_pending_reqs[idx].requester_node_id = local_node_id;
                                        gpd_pending_reqs[idx].type = packet.type;
                                        gpd_pending_reqs[idx].client_fd = -1;
                                        gpd_pending_reqs[idx].expected_acks = 0;
                                    }
                                    struct dsm_packet fetch_packet;
                                    fetch_packet.type = MSG_REQUEST_PAGE_READ;
                                    fetch_packet.page_index = idx;
                                    fetch_packet.sender_node_id = local_node_id;
                                    memset(fetch_packet.payload, 0, 4096);
                                    send_all_nonblocking(node_fds[old_owner], &fetch_packet, sizeof(struct dsm_packet));
                                } else {
                                    pthread_mutex_lock(&page_table[idx].lock);
                                    struct uffdio_copy uffd_copy;
                                    memset(&uffd_copy, 0, sizeof(uffd_copy));
                                    uffd_copy.dst = (unsigned long)page_table[idx].base_addr;
                                    uffd_copy.src = (unsigned long)packet.payload;
                                    uffd_copy.len = page_size;
                                    uffd_copy.mode = 0;
                                    if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0 && errno == EEXIST) {
                                        madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                        ioctl(uffd, UFFDIO_COPY, &uffd_copy);
                                    }
                                    page_table[idx].status = PAGE_VALID;
                                    dsm_mark_accessed(idx);
                                    pthread_mutex_unlock(&page_table[idx].lock);

                                    uint64_t invalid_mask = 0;
                                    if (out_req.is_write) {
                                        gpd_transition_to_modified(idx, local_node_id, &invalid_mask);
                                    } else {
                                        uint32_t dummy;
                                        gpd_transition_to_shared(idx, local_node_id, &dummy);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else {
                int client_fd = current_session->fd;

                if (epoll_events[i].events & EPOLLOUT) {
                    int sock_err = 0;
                    socklen_t err_len = sizeof(sock_err);
                    if (getsockopt(client_fd, SOL_SOCKET, SO_ERROR, &sock_err, &err_len) < 0 || sock_err != 0) {
                       
                        for (uint32_t m = 0; m < 64; m++) {
                            if (node_fds[m] == client_fd) {
                                node_fds[m] = -1;
                            }
                        }
                        if (client_fd == coordinator_fd) {
                            coordinator_fd = -1;
                        }
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                        close(client_fd);
                        free(current_session);
                        continue;
                    }

                    struct epoll_event write_ev;
                    write_ev.events = EPOLLIN | EPOLLET; 
                    write_ev.data.ptr = current_session;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &write_ev);
                    continue;
                }

                size_t target_size = sizeof(struct dsm_packet);
                uint8_t *raw_buf = (uint8_t *)&current_session->packet_buffer;
                int connection_closed = 0;
                int read_done = 0;

                while (!connection_closed && !read_done) {
                    ssize_t bytes_received = recv(client_fd,
                                                  raw_buf + current_session->bytes_read,
                                                  target_size - current_session->bytes_read,
                                                  0);

                    if (bytes_received < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            read_done = 1;
                        } else {
                            connection_closed = 1;
                        }
                    } 
                    else if (bytes_received == 0) {
                        connection_closed = 1;
                    } 
                    else {
                        current_session->bytes_read += bytes_received;
                        if (current_session->bytes_read == target_size) {
                            uint64_t idx = current_session->packet_buffer.page_index;
                            
                            if (current_session->packet_buffer.type == MSG_NODE_JOIN) {
                                uint32_t joining_node = current_session->packet_buffer.sender_node_id;
                                if (joining_node < 64) {
                                    node_fds[joining_node] = client_fd;
                                }
                            }
                            else if (current_session->packet_buffer.type == MSG_SEND_PAGE) {
                                if (idx < num_pages) {
                                    if (coordinator_fd == -1 && gpd_pending_reqs && gpd_pending_reqs[idx].requester_node_id != 0) {
                                        uint32_t req_node = gpd_pending_reqs[idx].requester_node_id;
                                        uint8_t req_type = gpd_pending_reqs[idx].type;
                                        int req_fd = gpd_pending_reqs[idx].client_fd;

                                        if (req_type == MSG_REQUEST_PAGE_WRITE) {
                                            uint64_t invalid_mask = 0;
                                            gpd_transition_to_modified(idx, req_node, &invalid_mask);

                                            if ((invalid_mask & (1ULL << local_node_id)) && local_node_id != req_node) {
                                                invalidate_local_page_copy(idx);
                                                invalid_mask &= ~(1ULL << local_node_id);
                                            }
                                            
                                            uint32_t ack_count = 0;
                                            for (uint32_t m = 0; m < 64; m++) {
                                                if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != req_node) {
                                                    ack_count++;
                                                }
                                            }

                                            if (ack_count > 0) {
                                                gpd_pending_reqs[idx].expected_acks = ack_count;
                                                memcpy(gpd_pending_reqs[idx].deferred_payload, current_session->packet_buffer.payload, 4096);
                                                
                                                for (uint32_t m = 0; m < 64; m++) {
                                                    if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != req_node) {
                                                        struct dsm_packet inv_packet;
                                                        inv_packet.type = MSG_INVALIDATE;
                                                        inv_packet.page_index = idx;
                                                        inv_packet.sender_node_id = 0;
                                                        memset(inv_packet.payload, 0, 4096);
                                                        send_all_nonblocking(node_fds[m], &inv_packet, sizeof(struct dsm_packet));
                                                    }
                                                }
                                            } else {
                                                if (req_node == local_node_id) {
                                                    pthread_mutex_lock(&page_table[idx].lock);
                                                    struct uffdio_copy uffd_copy;
                                                    memset(&uffd_copy, 0, sizeof(uffd_copy));
                                                    uffd_copy.dst = (unsigned long)page_table[idx].base_addr;
                                                    uffd_copy.src = (unsigned long)current_session->packet_buffer.payload;
                                                    uffd_copy.len = page_size;
                                                    uffd_copy.mode = 0;
                                                    if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0 && errno == EEXIST) {
                                                        madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                                        ioctl(uffd, UFFDIO_COPY, &uffd_copy);
                                                    }
                                                    page_table[idx].status = PAGE_VALID;
                                                    dsm_mark_accessed(idx);
                                                    pthread_mutex_unlock(&page_table[idx].lock);
                                                } else {
                                                    struct dsm_packet forward_pkt = current_session->packet_buffer;
                                                    send_all_nonblocking(req_fd, &forward_pkt, sizeof(struct dsm_packet));
                                                }
                                                gpd_pending_reqs[idx].requester_node_id = 0;
                                            }
                                        } else {
                                            uint32_t old_owner = 0;
                                            gpd_transition_to_shared(idx, req_node, &old_owner);
                                            
                                            if (req_node == local_node_id) {
                                                pthread_mutex_lock(&page_table[idx].lock);
                                                struct uffdio_copy uffd_copy;
                                                memset(&uffd_copy, 0, sizeof(uffd_copy));
                                                uffd_copy.dst = (unsigned long)page_table[idx].base_addr;
                                                uffd_copy.src = (unsigned long)current_session->packet_buffer.payload;
                                                uffd_copy.len = page_size;
                                                uffd_copy.mode = 0;
                                                if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0 && errno == EEXIST) {
                                                    madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                                    ioctl(uffd, UFFDIO_COPY, &uffd_copy);
                                                }
                                                page_table[idx].status = PAGE_VALID;
                                                dsm_mark_accessed(idx);
                                                pthread_mutex_unlock(&page_table[idx].lock);
                                            } else {
                                                struct dsm_packet forward_pkt = current_session->packet_buffer;
                                                send_all_nonblocking(req_fd, &forward_pkt, sizeof(struct dsm_packet));
                                            }
                                            gpd_pending_reqs[idx].requester_node_id = 0;
                                        }
                                    } else {
                                        pthread_mutex_lock(&page_table[idx].lock);
                                        /*
                                         * Accept PAGE_PENDING        – normal fault-driven fetch
                                         * Accept PAGE_WRITE_PENDING  – write-upgrade in flight
                                         * Accept PAGE_EMPTY          – unsolicited push: coordinator
                                         *   sent fresh data after an invalidate clears our mapping.
                                         *   Without this, the faulting thread hangs forever.
                                         */
                                        if (page_table[idx].status == PAGE_PENDING  ||
                                            page_table[idx].status == PAGE_WRITE_PENDING ||
                                            page_table[idx].status == PAGE_EMPTY) {
                                            if (page_table[idx].status == PAGE_WRITE_PENDING) {
                                                struct uffdio_writeprotect wp;
                                                wp.range.start = (unsigned long)page_table[idx].base_addr;
                                                wp.range.len = page_size;
                                                wp.mode = 0;
                                                ioctl(uffd, UFFDIO_WRITEPROTECT, &wp);
                                            } else {
                                                struct uffdio_copy uffd_copy;
                                                memset(&uffd_copy, 0, sizeof(uffd_copy));
                                                uffd_copy.dst = (unsigned long)page_table[idx].base_addr;
                                                uffd_copy.src = (unsigned long)current_session->packet_buffer.payload;
                                                uffd_copy.len = page_size;
                                                uffd_copy.mode = 0;
                                                if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0 && errno == EEXIST) {
                                                    madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                                    ioctl(uffd, UFFDIO_COPY, &uffd_copy);
                                                }
                                            }
                                            page_table[idx].status = PAGE_VALID;
                                            dsm_mark_accessed(idx);
                                        }
                                        pthread_mutex_unlock(&page_table[idx].lock);
                                    }
                                }
                            } 
                            else if (current_session->packet_buffer.type == MSG_INVALIDATE) {
                                if (idx < num_pages) {
                                    pthread_mutex_lock(&page_table[idx].lock);
                                    madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                    page_table[idx].status = PAGE_EMPTY;
                                    pthread_mutex_unlock(&page_table[idx].lock);

                                    struct dsm_packet ack_pkt;
                                    ack_pkt.type = MSG_INVALIDATE_ACK;
                                    ack_pkt.page_index = idx;
                                    ack_pkt.sender_node_id = local_node_id;
                                    memset(ack_pkt.payload, 0, 4096);

                                    if (coordinator_fd != -1) {
                                        send_all_nonblocking(coordinator_fd, &ack_pkt, sizeof(struct dsm_packet));
                                    } else {
                                        send_all_nonblocking(client_fd, &ack_pkt, sizeof(struct dsm_packet));
                                    }
                                }
                            }
                            else if (current_session->packet_buffer.type == MSG_INVALIDATE_ACK) {
                                if (idx < num_pages) {
                                    if (gpd_pending_reqs && gpd_pending_reqs[idx].expected_acks > 0) {
                                        gpd_pending_reqs[idx].expected_acks--;
                                        if (gpd_pending_reqs[idx].expected_acks == 0) {
                                            uint32_t req_node = gpd_pending_reqs[idx].requester_node_id;
                                            int req_fd = gpd_pending_reqs[idx].client_fd;

                                            if (req_node == local_node_id) {
                                                pthread_mutex_lock(&page_table[idx].lock);
                                                if (page_table[idx].status == PAGE_WRITE_PENDING) {
                                                    struct uffdio_writeprotect wp;
                                                    wp.range.start = (unsigned long)page_table[idx].base_addr;
                                                    wp.range.len = page_size;
                                                    wp.mode = 0;
                                                    ioctl(uffd, UFFDIO_WRITEPROTECT, &wp);
                                                    page_table[idx].status = PAGE_VALID;
                                                    dsm_mark_accessed(idx);
                                                } else {
                                                    struct uffdio_copy uffd_copy;
                                                    memset(&uffd_copy, 0, sizeof(uffd_copy));
                                                    uffd_copy.dst = (unsigned long)page_table[idx].base_addr;
                                                    uffd_copy.src = (unsigned long)gpd_pending_reqs[idx].deferred_payload;
                                                    uffd_copy.len = page_size;
                                                    uffd_copy.mode = 0;
                                                    if (ioctl(uffd, UFFDIO_COPY, &uffd_copy) < 0 && errno == EEXIST) {
                                                        madvise((void*)page_table[idx].base_addr, page_size, MADV_DONTNEED);
                                                        ioctl(uffd, UFFDIO_COPY, &uffd_copy);
                                                    }
                                                    page_table[idx].status = PAGE_VALID;
                                                    dsm_mark_accessed(idx);
                                                }
                                                pthread_mutex_unlock(&page_table[idx].lock);
                                            } else {
                                                struct dsm_packet response;
                                                response.type = MSG_SEND_PAGE;
                                                response.page_index = idx;
                                                response.sender_node_id = local_node_id;
                                                memcpy(response.payload, gpd_pending_reqs[idx].deferred_payload, page_size);
                                                send_all_nonblocking(req_fd, &response, sizeof(struct dsm_packet));
                                            }
                                            gpd_pending_reqs[idx].requester_node_id = 0;
                                        }
                                    }
                                }
                            }
                            else if (current_session->packet_buffer.type == MSG_REQUEST_PAGE_READ || current_session->packet_buffer.type == MSG_REQUEST_PAGE_WRITE) {
                                if (idx < num_pages) {
                                    uint32_t requesting_node = current_session->packet_buffer.sender_node_id;
                                    if (requesting_node < 64) {
                                        node_fds[requesting_node] = client_fd;
                                    }
                                    
                                    uint32_t old_owner = gpd_get_owner(idx);
                                    uint8_t req_type = current_session->packet_buffer.type;

                                    pthread_mutex_lock(&page_table[idx].lock);
                                    if (page_table[idx].status == PAGE_VALID) {
                                        struct dsm_packet response;
                                        response.type = MSG_SEND_PAGE;
                                        response.page_index = idx;
                                        response.sender_node_id = local_node_id;
                                        memcpy(response.payload, (void*)page_table[idx].base_addr, page_size);
                                        pthread_mutex_unlock(&page_table[idx].lock);

                                        if (req_type == MSG_REQUEST_PAGE_WRITE) {
                                            uint64_t invalid_mask = 0;
                                            gpd_transition_to_modified(idx, requesting_node, &invalid_mask);

                                            /* This node itself just supplied the page data above (it was our
                                             * own PAGE_VALID copy) and is now handing off MODIFIED ownership
                                             * to requesting_node. Drop our own mapping so we don't keep
                                             * serving/holding a stale copy after the handoff. */
                                            invalidate_local_page_copy(idx);
                                            invalid_mask &= ~(1ULL << local_node_id);
                                            
                                            uint32_t ack_count = 0;
                                            for (uint32_t m = 0; m < 64; m++) {
                                                if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != requesting_node) {
                                                    ack_count++;
                                                }
                                            }
                                            
                                            if (ack_count > 0) {
                                                gpd_pending_reqs[idx].requester_node_id = requesting_node;
                                                gpd_pending_reqs[idx].type = MSG_REQUEST_PAGE_WRITE;
                                                gpd_pending_reqs[idx].client_fd = client_fd;
                                                gpd_pending_reqs[idx].expected_acks = ack_count;
                                                memcpy(gpd_pending_reqs[idx].deferred_payload, response.payload, page_size);
                                                
                                                for (uint32_t m = 0; m < 64; m++) {
                                                    if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != requesting_node) {
                                                        struct dsm_packet inv_packet;
                                                        inv_packet.type = MSG_INVALIDATE;
                                                        inv_packet.page_index = idx;
                                                        inv_packet.sender_node_id = 0;
                                                        memset(inv_packet.payload, 0, 4096);
                                                        send_all_nonblocking(node_fds[m], &inv_packet, sizeof(struct dsm_packet));
                                                    }
                                                }
                                            } else {
                                                send_all_nonblocking(client_fd, &response, sizeof(struct dsm_packet));
                                            }
                                        } else {
                                            uint32_t prev_owner;
                                            gpd_transition_to_shared(idx, requesting_node, &prev_owner);

                                            /* We were the MODIFIED owner (our local copy is what we just
                                             * served above) and are now downgrading ourselves to SHARED
                                             * alongside requesting_node. Drop our local write permission
                                             * so a future store here faults and re-enters the coherence
                                             * protocol instead of silently diverging from the copy we
                                             * just handed out. */
                                            if (prev_owner == local_node_id) {
                                                writeprotect_local_page_copy(uffd, idx);
                                            }

                                            send_all_nonblocking(client_fd, &response, sizeof(struct dsm_packet));
                                        }
                                    } else {
                                        pthread_mutex_unlock(&page_table[idx].lock);
                                        if (old_owner != 0 && old_owner < 64 && node_fds[old_owner] != -1 && old_owner != requesting_node) {
                                            if (gpd_pending_reqs) {
                                                gpd_pending_reqs[idx].requester_node_id = requesting_node;
                                                gpd_pending_reqs[idx].type = req_type;
                                                gpd_pending_reqs[idx].client_fd = client_fd;
                                                gpd_pending_reqs[idx].expected_acks = 0;
                                            }

                                            struct dsm_packet fetch_packet;
                                            fetch_packet.type = MSG_REQUEST_PAGE_READ;
                                            fetch_packet.page_index = idx;
                                            fetch_packet.sender_node_id = local_node_id;
                                            memset(fetch_packet.payload, 0, 4096);
                                            send_all_nonblocking(node_fds[old_owner], &fetch_packet, sizeof(struct dsm_packet));
                                        } else {
                                            struct dsm_packet response;
                                            response.type = MSG_SEND_PAGE;
                                            response.page_index = idx;
                                            response.sender_node_id = local_node_id;
                                            memset(response.payload, 0, page_size);

                                            if (req_type == MSG_REQUEST_PAGE_WRITE) {
                                                uint64_t invalid_mask = 0;
                                                gpd_transition_to_modified(idx, requesting_node, &invalid_mask);

                                                if ((invalid_mask & (1ULL << local_node_id)) && local_node_id != requesting_node) {
                                                    invalidate_local_page_copy(idx);
                                                    invalid_mask &= ~(1ULL << local_node_id);
                                                }
                                                
                                                uint32_t ack_count = 0;
                                                for (uint32_t m = 0; m < 64; m++) {
                                                    if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != requesting_node) {
                                                        ack_count++;
                                                    }
                                                }
                                                
                                                if (ack_count > 0) {
                                                    gpd_pending_reqs[idx].requester_node_id = requesting_node;
                                                    gpd_pending_reqs[idx].type = MSG_REQUEST_PAGE_WRITE;
                                                    gpd_pending_reqs[idx].client_fd = client_fd;
                                                    gpd_pending_reqs[idx].expected_acks = ack_count;
                                                    memset(gpd_pending_reqs[idx].deferred_payload, 0, page_size);
                                                    
                                                    for (uint32_t m = 0; m < 64; m++) {
                                                        if ((invalid_mask & (1ULL << m)) && node_fds[m] != -1 && m != requesting_node) {
                                                            struct dsm_packet inv_packet;
                                                            inv_packet.type = MSG_INVALIDATE;
                                                            inv_packet.page_index = idx;
                                                            inv_packet.sender_node_id = 0;
                                                            memset(inv_packet.payload, 0, 4096);
                                                            send_all_nonblocking(node_fds[m], &inv_packet, sizeof(struct dsm_packet));
                                                        }
                                                    }
                                                } else {
                                                    send_all_nonblocking(client_fd, &response, sizeof(struct dsm_packet));
                                                }
                                            } else {
                                                uint32_t dummy;
                                                gpd_transition_to_shared(idx, requesting_node, &dummy);
                                                send_all_nonblocking(client_fd, &response, sizeof(struct dsm_packet));
                                            }
                                        }
                                    }
                                }
                            }

                            current_session->bytes_read = 0;
                            memset(&current_session->packet_buffer, 0, sizeof(struct dsm_packet));
                        }
                    }
                }

                if (connection_closed) {
                    
                    for (uint32_t m = 0; m < 64; m++) {
                        if (node_fds[m] == client_fd) {
                            node_fds[m] = -1;
                        }
                    }
                    if (client_fd == coordinator_fd) {
                        coordinator_fd = -1;
                    }
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
                    close(client_fd);
                    free(current_session);
                }
            }
        }
    }
    return NULL;
}
