#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define BUCKETS 1024
#define MAX_LINE 8192
#define MAX_EVENTS 64
#define MAX_CLIENTS 1024

typedef struct Entry {
    char *key, *value;
    long long expires_ms;
    struct Entry *next;
} Entry;

typedef struct HeapItem {
    long long when;
    Entry *entry;
} HeapItem;

static Entry *table[BUCKETS];
static HeapItem *heap;
static size_t heap_len, heap_cap;
static unsigned long long hits, misses, commands, clients_total, clients_active;
static time_t started;
static volatile sig_atomic_t stopping;

static long long now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (long long)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static unsigned hash_key(const char *s) {
    unsigned long h = 1469598103934665603ULL;
    while (*s) { h ^= (unsigned char)*s++; h *= 1099511628211ULL; }
    return (unsigned)(h % BUCKETS);
}

static void remove_entry(Entry *target) {
    if (!target) return;
    Entry **p = &table[hash_key(target->key)];
    while (*p) {
        if (*p == target) {
            *p = target->next;
            free(target->key); free(target->value); free(target);
            return;
        }
        p = &(*p)->next;
    }
}

static void heap_swap(size_t a, size_t b) {
    HeapItem x = heap[a]; heap[a] = heap[b]; heap[b] = x;
}

static int heap_push(long long when, Entry *e) {
    if (heap_len == heap_cap) {
        size_t cap = heap_cap ? heap_cap * 2 : 128;
        HeapItem *p = realloc(heap, cap * sizeof(*heap));
        if (!p) return -1;
        heap = p; heap_cap = cap;
    }
    size_t i = heap_len++;
    heap[i] = (HeapItem){when, e};
    while (i) {
        size_t parent = (i - 1) / 2;
        if (heap[parent].when <= heap[i].when) break;
        heap_swap(parent, i); i = parent;
    }
    return 0;
}

static HeapItem heap_pop(void) {
    HeapItem x = heap[0];
    heap[0] = heap[--heap_len];
    size_t i = 0;
    for (;;) {
        size_t l = i * 2 + 1, r = l + 1, m = i;
        if (l < heap_len && heap[l].when < heap[m].when) m = l;
        if (r < heap_len && heap[r].when < heap[m].when) m = r;
        if (m == i) break;
        heap_swap(i, m); i = m;
    }
    return x;
}

static void expire_due(void) {
    long long t = now_ms();
    while (heap_len && heap[0].when <= t) {
        HeapItem x = heap_pop();
        if (x.entry && x.entry->expires_ms == x.when) remove_entry(x.entry);
    }
}

static Entry *lookup(const char *key) {
    Entry *e = table[hash_key(key)];
    while (e) {
        if (!strcmp(e->key, key)) {
            if (e->expires_ms && e->expires_ms <= now_ms()) {
                remove_entry(e);
                return NULL;
            }
            return e;
        }
        e = e->next;
    }
    return NULL;
}

static int put(const char *key, const char *value, long long ttl_ms) {
    if (!*key || strlen(key) > 255 || strlen(value) > 4095) return -2;
    Entry *e = lookup(key);
    long long expiry = ttl_ms > 0 ? now_ms() + ttl_ms : 0;
    if (e) {
        char *v = strdup(value);
        if (!v) return -1;
        free(e->value); e->value = v; e->expires_ms = expiry;
        if (expiry && heap_push(expiry, e)) return -1;
        return 0;
    }
    e = calloc(1, sizeof(*e));
    if (!e || !(e->key = strdup(key)) || !(e->value = strdup(value))) {
        if (e) { free(e->key); free(e->value); free(e); }
        return -1;
    }
    e->expires_ms = expiry;
    unsigned b = hash_key(key);
    e->next = table[b]; table[b] = e;
    if (expiry && heap_push(expiry, e)) return -1;
    return 0;
}

static int send_all(int fd, const char *s) {
    size_t n = strlen(s);
    while (n) {
        ssize_t w = send(fd, s, n, MSG_NOSIGNAL);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        s += w; n -= (size_t)w;
    }
    return 0;
}

static int split(char *line, char **a, int max) {
    int n = 0; char *save = NULL;
    for (char *p = strtok_r(line, " \t\r\n", &save);
         p && n < max; p = strtok_r(NULL, " \t\r\n", &save)) a[n++] = p;
    return n;
}

static int number(const char *s, long long *v) {
    char *end; errno = 0; *v = strtoll(s, &end, 10);
    return errno || !*s || *end;
}

static int glob(const char *p, const char *s) {
    if (!*p) return !*s;
    if (*p == '*') return glob(p + 1, s) || (*s && glob(p, s + 1));
    if (*p == '?') return *s && glob(p + 1, s + 1);
    return *p == *s && glob(p + 1, s + 1);
}

static long long key_count(void) {
    long long n = 0;
    for (int i = 0; i < BUCKETS; ++i)
        for (Entry *e = table[i]; e; e = e->next) ++n;
    return n;
}

static int save_file(const char *path) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    expire_due();
    for (int i = 0; i < BUCKETS; ++i)
        for (Entry *e = table[i]; e; e = e->next) {
            long long ttl = e->expires_ms ? e->expires_ms - now_ms() : 0;
            if (ttl < 0) continue;
            fprintf(f, "%s\t%s\t%lld\n", e->key, e->value, ttl);
        }
    fclose(f);
    return 0;
}

static int load_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char line[MAX_LINE];
    while (fgets(line, sizeof line, f)) {
        char *k = strtok(line, "\t\r\n");
        char *v = strtok(NULL, "\t\r\n");
        char *t = strtok(NULL, "\t\r\n");
        long long ttl;
        if (!k || !v || !t || number(t, &ttl)) continue;
        if (put(k, v, ttl)) { fclose(f); return -1; }
    }
    fclose(f);
    return 0;
}

static void info(char *out, size_t cap) {
    snprintf(out, cap,
        "keys:%lld\nhits:%llu\nmisses:%llu\ncommands:%llu\nclients:%llu\nactive_clients:%llu\nuptime:%lld\n",
        key_count(), hits, misses, commands, clients_total, clients_active,
        (long long)(time(NULL) - started));
}

static int command(int fd, char *line) {
    char *a[8]; int n = split(line, a, 8);
    if (!n) return 0;
    for (char *p = a[0]; *p; ++p) *p = (char)toupper((unsigned char)*p);
    ++commands;

    if (!strcmp(a[0], "PING")) return send_all(fd, "+PONG\n");
    if (!strcmp(a[0], "QUIT")) { send_all(fd, "+OK\n"); return 1; }

    if (!strcmp(a[0], "SET")) {
        if (n < 3) return send_all(fd, "-ERR usage: SET key value [EX seconds|PX milliseconds]\n");
        long long ttl = 0, x;
        if (n >= 5) {
            if (number(a[4], &x) || x <= 0 ||
                (strcasecmp(a[3], "EX") && strcasecmp(a[3], "PX")))
                return send_all(fd, "-ERR invalid expiration\n");
            ttl = !strcasecmp(a[3], "EX") ? x * 1000 : x;
        }
        int rc = put(a[1], a[2], ttl);
        if (rc == -2) return send_all(fd, "-ERR key/value too long\n");
        if (rc) return send_all(fd, "-ERR out of memory\n");
        return send_all(fd, "+OK\n");
    }

    if (!strcmp(a[0], "GET")) {
        if (n != 2) return send_all(fd, "-ERR usage: GET key\n");
        Entry *e = lookup(a[1]);
        if (!e) { ++misses; return send_all(fd, "$-1\n"); }
        ++hits;
        char out[4160];
        snprintf(out, sizeof out, "$%zu\n%s\n", strlen(e->value), e->value);
        return send_all(fd, out);
    }

    if (!strcmp(a[0], "DEL")) {
        if (n != 2) return send_all(fd, "-ERR usage: DEL key\n");
        Entry *e = lookup(a[1]);
        if (!e) return send_all(fd, ":0\n");
        remove_entry(e); return send_all(fd, ":1\n");
    }

    if (!strcmp(a[0], "EXISTS")) {
        if (n != 2) return send_all(fd, "-ERR usage: EXISTS key\n");
        return send_all(fd, lookup(a[1]) ? ":1\n" : ":0\n");
    }

    if (!strcmp(a[0], "INCR") || !strcmp(a[0], "DECR")) {
        if (n != 2) return send_all(fd, "-ERR usage: INCR/DECR key\n");
        Entry *e = lookup(a[1]); long long v = 0;
        if (e && number(e->value, &v)) return send_all(fd, "-ERR value is not an integer\n");
        v += !strcmp(a[0], "INCR") ? 1 : -1;
        char s[64]; snprintf(s, sizeof s, "%lld", v);
        if (put(a[1], s, 0)) return send_all(fd, "-ERR out of memory\n");
        snprintf(s, sizeof s, ":%lld\n", v); return send_all(fd, s);
    }

    if (!strcmp(a[0], "TTL") || !strcmp(a[0], "TTLMS")) {
        if (n != 2) return send_all(fd, "-ERR usage: TTL/TTLMS key\n");
        Entry *e = lookup(a[1]);
        if (!e) return send_all(fd, ":-2\n");
        if (!e->expires_ms) return send_all(fd, ":-1\n");
        long long left = e->expires_ms - now_ms();
        if (left < 0) left = 0;
        if (!strcmp(a[0], "TTL")) left /= 1000;
        char s[64]; snprintf(s, sizeof s, ":%lld\n", left); return send_all(fd, s);
    }

    if (!strcmp(a[0], "KEYS")) {
        if (n != 2) return send_all(fd, "-ERR usage: KEYS pattern\n");
        expire_due();
        char out[MAX_LINE]; size_t used = 0; int first = 1;
        for (int i = 0; i < BUCKETS; ++i)
            for (Entry *e = table[i]; e; e = e->next)
                if (glob(a[1], e->key)) {
                    int w = snprintf(out + used, sizeof out - used, "%s%s",
                                     first ? "" : " ", e->key);
                    if (w < 0 || (size_t)w >= sizeof out - used) return send_all(fd, "-ERR result too large\n");
                    used += (size_t)w; first = 0;
                }
        if (used + 2 >= sizeof out) return send_all(fd, "-ERR result too large\n");
        out[used++] = '\n'; out[used] = 0; return send_all(fd, out);
    }

    if (!strcmp(a[0], "INFO") || !strcmp(a[0], "STATS")) {
        char out[512]; info(out, sizeof out); return send_all(fd, out);
    }

    if (!strcmp(a[0], "SAVE")) {
        if (n != 2) return send_all(fd, "-ERR usage: SAVE path\n");
        return send_all(fd, save_file(a[1]) ? "-ERR save failed\n" : "+OK\n");
    }

    if (!strcmp(a[0], "LOAD")) {
        if (n != 2) return send_all(fd, "-ERR usage: LOAD path\n");
        return send_all(fd, load_file(a[1]) ? "-ERR load failed\n" : "+OK\n");
    }

    return send_all(fd, "-ERR unknown command\n");
}

static void stop_handler(int sig) { (void)sig; stopping = 1; }

static int listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int yes = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) || listen(fd, 128)) {
        close(fd); return -1;
    }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    return fd;
}

static void close_client(int ep, int fd) {
    epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    if (clients_active) --clients_active;
}

int main(int argc, char **argv) {
    signal(SIGINT, stop_handler); signal(SIGTERM, stop_handler); signal(SIGPIPE, SIG_IGN);
    started = time(NULL);
    int port = argc > 1 ? atoi(argv[1]) : 6379;
    int listen_fd = listener(port);
    if (listen_fd < 0) { perror("listen"); return 1; }

    int ep = epoll_create1(0);
    if (ep < 0) { perror("epoll_create1"); close(listen_fd); return 1; }

    struct epoll_event e = {.events = EPOLLIN, .data.fd = listen_fd};
    epoll_ctl(ep, EPOLL_CTL_ADD, listen_fd, &e);
    fprintf(stderr, "RAMORA listening on 0.0.0.0:%d\n", port);

    static char buffers[MAX_CLIENTS][MAX_LINE];
    static size_t lengths[MAX_CLIENTS];
    struct epoll_event events[MAX_EVENTS];

    while (!stopping) {
        expire_due();
        int n = epoll_wait(ep, events, MAX_EVENTS, 500);
        if (n < 0) { if (errno == EINTR) continue; break; }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == listen_fd) {
                for (;;) {
                    int c = accept(listen_fd, NULL, NULL);
                    if (c < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) break; break; }
                    if (c >= MAX_CLIENTS) { close(c); continue; }
                    fcntl(c, F_SETFL, fcntl(c, F_GETFL) | O_NONBLOCK);
                    lengths[c] = 0;
                    struct epoll_event ce = {.events = EPOLLIN | EPOLLRDHUP, .data.fd = c};
                    epoll_ctl(ep, EPOLL_CTL_ADD, c, &ce);
                    ++clients_total; ++clients_active;
                }
                continue;
            }

            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                close_client(ep, fd); continue;
            }

            ssize_t r = recv(fd, buffers[fd] + lengths[fd],
                             MAX_LINE - 1 - lengths[fd], 0);
            if (r <= 0) { close_client(ep, fd); continue; }
            lengths[fd] += (size_t)r; buffers[fd][lengths[fd]] = 0;

            char *start = buffers[fd], *nl;
            while ((nl = strchr(start, '\n'))) {
                *nl = 0;
                if (command(fd, start)) { close_client(ep, fd); start = NULL; break; }
                start = nl + 1;
            }
            if (start) {
                size_t left = strlen(start);
                memmove(buffers[fd], start, left + 1);
                lengths[fd] = left;
            }
        }
    }

    close(listen_fd); close(ep); free(heap);
    for (int i = 0; i < BUCKETS; ++i) {
        Entry *e = table[i];
        while (e) { Entry *next = e->next; free(e->key); free(e->value); free(e); e = next; }
    }
    return 0;
}
