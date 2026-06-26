#include "queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/eventfd.h>

void queue_init(request_queue_t *q) {
    q->head = 0;
    q->tail = 0;
    q->size = 0;
    pthread_mutex_init(&q->lock, NULL);
    q->notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (q->notify_fd < 0) {
        perror("eventfd error");
        exit(1);
    }
}

int queue_push(request_queue_t *q, page_request_t req) {
    pthread_mutex_lock(&q->lock);
    if (q->size == QUEUE_CAPACITY) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    q->data[q->tail] = req;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;
    q->size++;
    uint64_t u = 1;
    //write(q->notify_fd, &u, sizeof(u));
    ssize_t unused = write(q->notify_fd, &u, sizeof(u));
    (void)unused;
    pthread_mutex_unlock(&q->lock);
    return 0;
}

int queue_pop(request_queue_t *q, page_request_t *out_req) {
    pthread_mutex_lock(&q->lock);
    if (q->size == 0) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    *out_req = q->data[q->head];
    q->head = (q->head + 1) % QUEUE_CAPACITY;
    q->size--;
    pthread_mutex_unlock(&q->lock);
    return 0;
}
