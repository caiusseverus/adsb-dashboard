# Makefile for libdecode — readsb-derived Mode-S decode shared library.
#
# Usage:
#   make            → build libdecode.so in this directory
#   make clean      → remove build artefacts
#   make install    → copy libdecode.so to ../  (alongside Python package)
#
# Targets aarch64 (Raspberry Pi 4/5) and x86-64.  The -march=native flag
# lets the compiler use NEON / AVX2 as available; omit it for a portable build.

CC      ?= gcc
CFLAGS  := -O2 -march=native -fPIC -Wall -Wextra -Wno-unused-parameter \
           -std=c11 -D_GNU_SOURCE
LDFLAGS := -shared -lm

TARGET  := libdecode.so

SRCS := decode_api.c \
        radar_burst.c \
        mode_s.c     \
        mode_ac.c    \
        crc.c        \
        cpr.c        \
        comm_b.c     \
        icao_filter.c \
        ais_charset.c \
        fasthash.c

OBJS := $(SRCS:.c=.o)

.PHONY: all clean install

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

install: $(TARGET)
	cp $(TARGET) ../$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
