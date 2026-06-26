#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>
#include <assert.h>

#define MATRIX_DIM 64
#define PAGE_SIZE 4096

typedef struct {
    uint32_t *matrix_data;
    size_t start_row;
    size_t end_row;
    uint64_t *fault_latencies;
    size_t fault_count;
} thread_arg_t;

void *matrix_transform_worker(void *arg) {
    thread_arg_t *t_data = (thread_arg_t *)arg;
    size_t elements_per_row = MATRIX_DIM;

    for (size_t i = t_data->start_row; i < t_data->end_row; i++) {
        for (size_t j = 0; j < elements_per_row; j++) {
            struct timespec start_fault, end_fault;
            size_t linear_index = i * elements_per_row + j;
            
            clock_gettime(CLOCK_MONOTONIC, &start_fault);
            
            t_data->matrix_data[linear_index] = (uint32_t)(linear_index * 3);
            
            clock_gettime(CLOCK_MONOTONIC, &end_fault);

            uint64_t duration_ns = (uint64_t)(end_fault.tv_sec - start_fault.tv_sec) * 1000000000ULL +
                                   (uint64_t)(end_fault.tv_nsec - start_fault.tv_nsec);
            
            if (duration_ns > 500000ULL) {
                if (t_data->fault_count < 1024) {
                    t_data->fault_latencies[t_data->fault_count++] = duration_ns;
                }
            }
        }
    }
    return NULL;
}

void execute_cluster_benchmark(void *dsm_shared_region, size_t region_size) {
    struct timespec start_total, end_total;
    pthread_t threads[4];
    thread_arg_t args[4];
    uint64_t shared_latencies[4][1024] = {0};
    size_t rows_per_thread = MATRIX_DIM / 4;

    uint32_t *matrix = (uint32_t *)dsm_shared_region;
    assert(region_size >= (MATRIX_DIM * MATRIX_DIM * sizeof(uint32_t)));

    clock_gettime(CLOCK_MONOTONIC, &start_total);

    for (int i = 0; i < 4; i++) {
        args[i].matrix_data = matrix;
        args[i].start_row = i * rows_per_thread;
        args[i].end_row = (i + 1) * rows_per_thread;
        args[i].fault_latencies = shared_latencies[i];
        args[i].fault_count = 0;

        pthread_create(&threads[i], NULL, matrix_transform_worker, &args[i]);
    }

    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end_total);

    uint64_t total_elapsed_ns = (uint64_t)(end_total.tv_sec - start_total.tv_sec) * 1000000000ULL +
                                 (uint64_t)(end_total.tv_nsec - start_total.tv_nsec);

    uint64_t combined_latency = 0;
    size_t total_faults_recorded = 0;
    for (int i = 0; i < 4; i++) {
        total_faults_recorded += args[i].fault_count;
        for (size_t j = 0; j < args[i].fault_count; j++) {
            combined_latency += shared_latencies[i][j];
        }
    }

    printf("METRIC_TOTAL_DURATION_NS:%lu\n", total_elapsed_ns);
    if (total_faults_recorded > 0) {
        printf("METRIC_AVG_FAULT_LATENCY_NS:%lu\n", combined_latency / total_faults_recorded);
    }

    for (size_t i = 0; i < MATRIX_DIM * MATRIX_DIM; i++) {
        uint32_t expected_val = (uint32_t)(i * 3);
        if (matrix[i] != expected_val) {
            fprintf(stderr, "VALIDATION_FAILURE_AT_INDEX:%lu\n", i);
            exit(EXIT_FAILURE);
        }
    }
    printf("VALIDATION_STATUS:SUCCESS\n");
}
