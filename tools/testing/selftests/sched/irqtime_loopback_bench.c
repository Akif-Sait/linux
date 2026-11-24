// SPDX-License-Identifier: GPL-2.0-only
/*
 * Experimental TCP loopback scheduler-latency workload.
 * Research tool; not a Linux upstream selftest or evidence of a regression.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <stdbool.h>
#include <errno.h>
#include <getopt.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/utsname.h>
#include <unistd.h>

#define MAX_CLIENTS 64
#define MAX_SAMPLES 4000000UL

struct worker {
	int fd;
	int iterations;
	int payload;
	pthread_barrier_t *barrier;
	uint64_t *latencies;
	int error;
};

static atomic_bool stop_busy;

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static int transfer(int fd, char *p, size_t left, int sending)
{
	while (left) {
		ssize_t n = sending ? send(fd, p, left, MSG_NOSIGNAL) : recv(fd, p, left, 0);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			return -1;
		p += n;
		left -= (size_t)n;
	}
	return 0;
}

static void *echo_server(void *arg)
{
	struct worker *w = arg;
	char *buf = malloc((size_t)w->payload);
	if (!buf) { w->error = 1; return NULL; }
	for (int i = 0; i < w->iterations; ++i) {
		if (transfer(w->fd, buf, w->payload, 0) ||
		    transfer(w->fd, buf, w->payload, 1)) {
			w->error = 1;
			break;
		}
	}
	free(buf);
	return NULL;
}

static void *ping_client(void *arg)
{
	struct worker *w = arg;
	char *send_buf = malloc((size_t)w->payload);
	char *recv_buf = malloc((size_t)w->payload);
	if (!send_buf || !recv_buf) {
		w->error = 1;
		free(send_buf);
		free(recv_buf);
		pthread_barrier_wait(w->barrier);
		return NULL;
	}
	memset(send_buf, 0xa5, (size_t)w->payload);
	pthread_barrier_wait(w->barrier);
	for (int i = 0; i < w->iterations; ++i) {
		uint64_t start = now_ns();
		if (transfer(w->fd, send_buf, w->payload, 1) ||
		    transfer(w->fd, recv_buf, w->payload, 0) ||
		    memcmp(send_buf, recv_buf, (size_t)w->payload)) {
			w->error = 1;
			break;
		}
		w->latencies[i] = now_ns() - start;
	}
	free(recv_buf);
	free(send_buf);
	return NULL;
}

static void *spin_worker(void *unused)
{
	uint64_t x = 0x9e3779b97f4a7c15ULL;
	(void)unused;
	while (!atomic_load_explicit(&stop_busy, memory_order_relaxed)) {
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		__asm__ volatile("" : "+r" (x));
	}
	return NULL;
}

static int compare_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return (x > y) - (x < y);
}

static uint64_t percentile(uint64_t *x, size_t n, unsigned per_mille)
{
	/* Nearest-rank percentile: ceil(n*p/1000), one-indexed. */
	if (!n) return 0;
	return x[((uint64_t)n * per_mille + 999) / 1000 - 1];
}

static int make_pair(int listener, int *client, int *server)
{
	struct sockaddr_in addr = { .sin_family = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = 0 };
	socklen_t len = sizeof(addr);
	if (getsockname(listener, (void *)&addr, &len)) return -1;
	*client = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (*client < 0) return -1;
	if (connect(*client, (void *)&addr, len)) return -1;
	*server = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
	if (*server < 0) return -1;
	struct timeval timeout = { .tv_sec = 10 };
	setsockopt(*client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
	setsockopt(*server, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
	return 0;
}

static void usage(const char *name)
{
	fprintf(stderr, "Usage: %s [-c clients:1..64] [-n iterations] [-p payload:1..4096] [-b busy_threads:0..64]\n", name);
}

int main(int argc, char **argv)
{
	int clients = 4, iterations = 10000, payload = 64, busy = 0;
	int opt, listener = -1, error = 0;
	struct sockaddr_in addr = { .sin_family = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = 0 };
	pthread_t *client_threads, *server_threads, *busy_threads;
	struct worker *c, *s;
	pthread_barrier_t barrier;
	struct utsname uts;
	uint64_t *samples, start, end, total = 0, max = 0, count = 0;

	while ((opt = getopt(argc, argv, "c:n:p:b:h")) != -1) {
		switch (opt) {
		case 'c': clients = atoi(optarg); break;
		case 'n': iterations = atoi(optarg); break;
		case 'p': payload = atoi(optarg); break;
		case 'b': busy = atoi(optarg); break;
		default: usage(argv[0]); return opt == 'h' ? 0 : 2;
		}
	}
	if (clients < 1 || clients > MAX_CLIENTS || iterations < 1 ||
	    (size_t)clients * (size_t)iterations > MAX_SAMPLES ||
	    payload < 1 || payload > 4096 || busy < 0 || busy > 64) {
		usage(argv[0]); return 2;
	}

	client_threads = calloc((size_t)clients, sizeof(*client_threads));
	server_threads = calloc((size_t)clients, sizeof(*server_threads));
	busy_threads = calloc((size_t)(busy ? busy : 1), sizeof(*busy_threads));
	c = calloc((size_t)clients, sizeof(*c));
	s = calloc((size_t)clients, sizeof(*s));
	samples = calloc((size_t)clients * (size_t)iterations, sizeof(*samples));
	if (!client_threads || !server_threads || !busy_threads || !c || !s || !samples) {
		perror("allocation"); return 1;
	}
	listener = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (listener < 0 || bind(listener, (void *)&addr, sizeof(addr)) ||
	    listen(listener, clients)) {
		perror("listen"); return 1;
	}
	pthread_barrier_init(&barrier, NULL, (unsigned)clients + 1);
	for (int i = 0; i < busy; ++i)
		if (pthread_create(&busy_threads[i], NULL, spin_worker, NULL)) return 1;
	for (int i = 0; i < clients; ++i) {
		if (make_pair(listener, &c[i].fd, &s[i].fd)) { perror("connect/accept"); return 1; }
		c[i].payload = s[i].payload = payload;
		c[i].iterations = s[i].iterations = iterations;
		c[i].barrier = &barrier;
		c[i].latencies = samples + (size_t)i * (size_t)iterations;
		if (pthread_create(&server_threads[i], NULL, echo_server, &s[i]) ||
		    pthread_create(&client_threads[i], NULL, ping_client, &c[i])) return 1;
	}
	start = now_ns();
	pthread_barrier_wait(&barrier);
	for (int i = 0; i < clients; ++i) pthread_join(client_threads[i], NULL);
	end = now_ns();
	for (int i = 0; i < clients; ++i) pthread_join(server_threads[i], NULL);
	atomic_store(&stop_busy, true);
	for (int i = 0; i < busy; ++i) pthread_join(busy_threads[i], NULL);
	for (int i = 0; i < clients; ++i) {
		close(c[i].fd);
		close(s[i].fd);
		if (c[i].error || s[i].error) error = 1;
		for (int j = 0; j < iterations; ++j) {
			uint64_t v = c[i].latencies[j];
			if (!v) { error = 1; break; }
			total += v; count++;
			if (v > max) max = v;
		}
	}
	if (!error) {
		if (uname(&uts)) return 1;
		qsort(samples, (size_t)clients * (size_t)iterations, sizeof(*samples), compare_u64);
		printf("kernel=%s, clients=%d, iterations=%d, payload=%d, busy=%d, ops=%llu, elapsed_s=%.3f\n",
			uts.release, clients, iterations, payload, busy,
			(unsigned long long)count, (end-start)/1e9);
		printf("avg_us=%.3f p50_us=%.3f p95_us=%.3f p99_us=%.3f p99.9_us=%.3f max_us=%.3f\n",
			total/(double)count/1e3,
			percentile(samples,count,500)/1e3, percentile(samples,count,950)/1e3,
			percentile(samples,count,990)/1e3, percentile(samples,count,999)/1e3, max/1e3);
	}
	pthread_barrier_destroy(&barrier);
	close(listener);
	free(client_threads); free(server_threads); free(busy_threads);
	free(c); free(s); free(samples);
	return error;
}
