
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include "dsm_core.h"

void run_coherence_test_loop(void *region, size_t region_size) {
    (void)region_size;
    char line[256];

    printf("[CoherenceTest] Ready. Waiting for commands on stdin...\n");
    fflush(stdout);

    while (fgets(line, sizeof(line), stdin)) {
        char cmd[16] = {0};
        unsigned long page_idx = 0;
        unsigned long value = 0;

        int n = sscanf(line, "%15s %lu %lu", cmd, &page_idx, &value);
        if (n < 1) continue;

        if (strcmp(cmd, "WRITE") == 0 && n >= 3) {
            if (page_idx * 4096 + sizeof(uint32_t) > REGION_SIZE) {
                printf("RESULT WRITE page=%lu ERROR=out_of_range\n", page_idx);
                fflush(stdout);
                continue;
            }
            uint32_t *slot = (uint32_t *)((uint8_t *)region + page_idx * 4096);
            *slot = (uint32_t)value;
            printf("RESULT WRITE page=%lu value=%u\n", page_idx, *slot);
            fflush(stdout);

        } else if (strcmp(cmd, "READ") == 0 && n >= 2) {
            if (page_idx * 4096 + sizeof(uint32_t) > REGION_SIZE) {
                printf("RESULT READ page=%lu ERROR=out_of_range\n", page_idx);
                fflush(stdout);
                continue;
            }
            uint32_t *slot = (uint32_t *)((uint8_t *)region + page_idx * 4096);
            uint32_t v = *slot;
            printf("RESULT READ page=%lu value=%u\n", page_idx, v);
            fflush(stdout);

        } else if (strcmp(cmd, "SLEEP") == 0 && n >= 2) {
            usleep((unsigned int)page_idx * 1000); /* reuses first %lu as ms */
            printf("RESULT SLEEP ms=%lu\n", page_idx);
            fflush(stdout);

        } else if (strcmp(cmd, "DONE") == 0) {
            printf("RESULT DONE\n");
            fflush(stdout);
            break;

        } else {
            printf("RESULT ERROR unknown_command=%s\n", cmd);
            fflush(stdout);
        }
    }
}
