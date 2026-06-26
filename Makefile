CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LIBS = -lpthread

TARGET = teleport_dsm
OBJS = queue.o dsm_core.o gpd.o fault_handler.o network_engine.o \
       eviction.o coordinator_sequencer.o benchmark.o coherence_test.o main.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
