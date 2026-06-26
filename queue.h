#ifndef QUEUE_H
#define QUEUE_H

#define QUEUE_CAPACITY 128
#include <stdint.h>
#include <pthread.h>

typedef struct {
    uint64_t page_index;
    int is_write;
} page_request_t;

typedef struct {
    page_request_t data[QUEUE_CAPACITY];
    int head;
    int tail;
    int size;
    pthread_mutex_t lock;
    int notify_fd;
} request_queue_t;

void queue_init(request_queue_t *q);
int queue_push(request_queue_t *q, page_request_t req);
int queue_pop(request_queue_t *q, page_request_t *out_req);

#endif
