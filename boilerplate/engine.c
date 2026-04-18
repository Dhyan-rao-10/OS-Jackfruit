#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 32
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record container_record_t;
typedef struct supervisor_ctx supervisor_ctx_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int monitor_registered;
    int log_read_fd;
    int client_fd;
    int wait_for_exit;
    int finished;
    void *child_stack;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    struct container_record *next;
};

struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    int should_stop;
    char base_rootfs[PATH_MAX];
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
};

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *container;
} producer_arg_t;

static volatile sig_atomic_t g_supervisor_stop = 0;

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    if (dst_size == 0)
        return;

    if (!src) {
        dst[0] = '\0';
        return;
    }

    snprintf(dst, dst_size, "%s", src);
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *ptr = buf;

    while (len > 0) {
        ssize_t written = write(fd, ptr, len);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        ptr += written;
        len -= (size_t)written;
    }

    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *ptr = buf;

    while (len > 0) {
        ssize_t read_rc = read(fd, ptr, len);
        if (read_rc == 0)
            return -1;
        if (read_rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        ptr += read_rc;
        len -= (size_t)read_rc;
    }

    return 0;
}

static void set_response(control_response_t *resp, int status, const char *fmt, ...)
{
    va_list ap;

    memset(resp, 0, sizeof(*resp));
    resp->status = status;

    va_start(ap, fmt);
    vsnprintf(resp->message, sizeof(resp->message), fmt, ap);
    va_end(ap);
}

static int send_response_fd(int fd, int status, const char *fmt, ...)
{
    control_response_t resp;
    va_list ap;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;

    va_start(ap, fmt);
    vsnprintf(resp.message, sizeof(resp.message), fmt, ap);
    va_end(ap);

    return write_all(fd, &resp, sizeof(resp));
}

static void close_quietly(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (!buffer->shutting_down && buffer->count == LOG_BUFFER_CAPACITY)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (cur->host_pid == pid)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (!cur->finished && strcmp(cur->rootfs, rootfs) == 0)
            return 1;
        cur = cur->next;
    }
    return 0;
}

static int active_container_count(supervisor_ctx_t *ctx)
{
    int count = 0;
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (!cur->finished)
            count++;
        cur = cur->next;
    }

    return count;
}

static int ensure_log_dir(void)
{
    if (mkdir(LOG_DIR, 0755) == 0)
        return 0;
    if (errno == EEXIST)
        return 0;
    return -1;
}

static void format_time_iso(time_t value, char *buf, size_t size)
{
    struct tm tm_value;

    if (localtime_r(&value, &tm_value) == NULL) {
        snprintf(buf, size, "unknown");
        return;
    }

    strftime(buf, size, "%Y-%m-%d %H:%M:%S", &tm_value);
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        container_record_t *container;
        int fd = -1;

        pthread_mutex_lock(&ctx->metadata_lock);
        container = find_container_by_id(ctx, item.container_id);
        if (container)
            fd = open(container->log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (fd >= 0) {
            (void)write_all(fd, item.data, item.length);
            close(fd);
        }
    }

    return NULL;
}

static int push_message(supervisor_ctx_t *ctx, const char *container_id, const char *data, size_t len)
{
    while (len > 0) {
        log_item_t item;
        size_t chunk = len > LOG_CHUNK_SIZE ? LOG_CHUNK_SIZE : len;

        memset(&item, 0, sizeof(item));
        copy_string(item.container_id, sizeof(item.container_id), container_id);
        memcpy(item.data, data, chunk);
        item.length = chunk;

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            return -1;

        data += chunk;
        len -= chunk;
    }

    return 0;
}

static int append_log_message(supervisor_ctx_t *ctx, const char *container_id, const char *fmt, ...)
{
    char buffer[512];
    va_list ap;
    int len;

    va_start(ap, fmt);
    len = vsnprintf(buffer, sizeof(buffer), fmt, ap);
    va_end(ap);

    if (len < 0)
        return -1;

    if ((size_t)len >= sizeof(buffer))
        len = (int)(sizeof(buffer) - 1);

    return push_message(ctx, container_id, buffer, (size_t)len);
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
        g_supervisor_stop = 1;
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static int ensure_directory(const char *path)
{
    if (mkdir(path, 0755) == 0)
        return 0;
    if (errno == EEXIST)
        return 0;
    return -1;
}

int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    if (cfg->nice_value != 0 && nice(cfg->nice_value) == -1 && errno != 0)
        perror("nice");

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount-private");

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }
    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    if (ensure_directory("/proc") < 0 && errno != EEXIST)
        perror("mkdir /proc");
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("exec");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

void *producer_thread_main(void *arg)
{
    producer_arg_t *producer = arg;
    supervisor_ctx_t *ctx = producer->ctx;
    container_record_t *container = producer->container;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t read_rc;

    free(producer);

    while ((read_rc = read(container->log_read_fd, buffer, sizeof(buffer))) > 0) {
        if (push_message(ctx, container->id, buffer, (size_t)read_rc) != 0)
            break;
    }

    close_quietly(&container->log_read_fd);
    return NULL;
}

static void send_final_run_response(container_record_t *container)
{
    if (!container->wait_for_exit || container->client_fd < 0)
        return;

    if (container->exit_signal != 0) {
        (void)send_response_fd(container->client_fd,
                               128 + container->exit_signal,
                               "container %s terminated by signal %d (%s)",
                               container->id,
                               container->exit_signal,
                               state_to_string(container->state));
    } else {
        (void)send_response_fd(container->client_fd,
                               container->exit_code,
                               "container %s exited with status %d (%s)",
                               container->id,
                               container->exit_code,
                               state_to_string(container->state));
    }

    close_quietly(&container->client_fd);
    container->wait_for_exit = 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           int client_fd,
                           int wait_for_exit,
                           control_response_t *resp)
{
    container_record_t *container;
    producer_arg_t *producer_arg;
    child_config_t *child_cfg = NULL;
    int pipe_fds[2] = {-1, -1};
    int rc;
    pid_t child_pid;

    if (ensure_log_dir() < 0) {
        set_response(resp, 1, "failed to create %s: %s", LOG_DIR, strerror(errno));
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        set_response(resp, 1, "container id '%s' already exists", req->container_id);
        return -1;
    }
    if (rootfs_in_use(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        set_response(resp, 1, "rootfs '%s' is already in use", req->rootfs);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipe_fds) < 0) {
        set_response(resp, 1, "pipe failed: %s", strerror(errno));
        return -1;
    }

    container = calloc(1, sizeof(*container));
    if (!container) {
        set_response(resp, 1, "calloc failed");
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }

    child_cfg = calloc(1, sizeof(*child_cfg));
    producer_arg = calloc(1, sizeof(*producer_arg));
    if (!child_cfg || !producer_arg) {
        set_response(resp, 1, "calloc failed");
        free(producer_arg);
        free(child_cfg);
        free(container);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }

    copy_string(container->id, sizeof(container->id), req->container_id);
    copy_string(container->rootfs, sizeof(container->rootfs), req->rootfs);
    snprintf(container->log_path, sizeof(container->log_path), LOG_DIR "/%s.log", req->container_id);
    container->state = CONTAINER_STARTING;
    container->soft_limit_bytes = req->soft_limit_bytes;
    container->hard_limit_bytes = req->hard_limit_bytes;
    container->log_read_fd = pipe_fds[0];
    container->client_fd = wait_for_exit ? client_fd : -1;
    container->wait_for_exit = wait_for_exit;

    copy_string(child_cfg->id, sizeof(child_cfg->id), req->container_id);
    copy_string(child_cfg->rootfs, sizeof(child_cfg->rootfs), req->rootfs);
    copy_string(child_cfg->command, sizeof(child_cfg->command), req->command);
    child_cfg->nice_value = req->nice_value;
    child_cfg->log_write_fd = pipe_fds[1];

    container->child_stack = malloc(STACK_SIZE);
    if (!container->child_stack) {
        set_response(resp, 1, "malloc stack failed");
        free(producer_arg);
        free(child_cfg);
        free(container);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }

    child_pid = clone(child_fn,
                      (char *)container->child_stack + STACK_SIZE,
                      CLONE_NEWNS | CLONE_NEWPID | CLONE_NEWUTS | SIGCHLD,
                      child_cfg);
    if (child_pid < 0) {
        set_response(resp, 1, "clone failed: %s", strerror(errno));
        free(container->child_stack);
        free(producer_arg);
        free(child_cfg);
        free(container);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }

    close(pipe_fds[1]);
    free(child_cfg);

    container->host_pid = child_pid;
    container->started_at = time(NULL);
    container->state = CONTAINER_RUNNING;

    producer_arg->ctx = ctx;
    producer_arg->container = container;
    rc = pthread_create(&container->producer_thread, NULL, producer_thread_main, producer_arg);
    if (rc != 0) {
        errno = rc;
        kill(child_pid, SIGKILL);
        set_response(resp, 1, "pthread_create failed: %s", strerror(errno));
        free(producer_arg);
        close_quietly(&container->log_read_fd);
        free(container->child_stack);
        free(container);
        return -1;
    }

    if (register_with_monitor(ctx->monitor_fd,
                              container->id,
                              container->host_pid,
                              container->soft_limit_bytes,
                              container->hard_limit_bytes) == 0) {
        container->monitor_registered = 1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    container->next = ctx->containers;
    ctx->containers = container;
    pthread_mutex_unlock(&ctx->metadata_lock);

    (void)append_log_message(ctx,
                             container->id,
                             "[supervisor] started pid=%d rootfs=%s command=%s soft=%lu hard=%lu nice=%d\n",
                             container->host_pid,
                             container->rootfs,
                             req->command,
                             container->soft_limit_bytes,
                             container->hard_limit_bytes,
                             req->nice_value);

    if (!wait_for_exit)
        set_response(resp, 0, "started container %s pid=%d", container->id, container->host_pid);

    return 0;
}

static void handle_child_exit(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *container;

    pthread_mutex_lock(&ctx->metadata_lock);
    container = find_container_by_pid(ctx, pid);
    if (!container) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    container->finished = 1;
    if (WIFEXITED(status)) {
        container->exit_code = WEXITSTATUS(status);
        container->state = container->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
    } else if (WIFSIGNALED(status)) {
        container->exit_signal = WTERMSIG(status);
        container->state = container->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (container->monitor_registered) {
        (void)unregister_from_monitor(ctx->monitor_fd, container->id, container->host_pid);
        container->monitor_registered = 0;
    }

    if (container->log_read_fd >= 0)
        pthread_join(container->producer_thread, NULL);

    (void)append_log_message(ctx,
                             container->id,
                             "[supervisor] exited pid=%d exit_code=%d exit_signal=%d state=%s\n",
                             container->host_pid,
                             container->exit_code,
                             container->exit_signal,
                             state_to_string(container->state));

    send_final_run_response(container);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    for (;;) {
        pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0)
            break;
        handle_child_exit(ctx, pid, status);
    }
}

static void write_ps_listing(supervisor_ctx_t *ctx, int fd)
{
    char buffer[CONTROL_MESSAGE_LEN];
    size_t used = 0;
    container_record_t *cur;

    used += (size_t)snprintf(buffer + used,
                             sizeof(buffer) - used,
                             "id\tpid\tstate\tstarted_at\tsoft_mib\thard_mib\texit\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur && used < sizeof(buffer)) {
        char started[64];

        format_time_iso(cur->started_at, started, sizeof(started));
        used += (size_t)snprintf(buffer + used,
                                 sizeof(buffer) - used,
                                 "%s\t%d\t%s\t%s\t%lu\t%lu\t%d/%d\n",
                                 cur->id,
                                 cur->host_pid,
                                 state_to_string(cur->state),
                                 started,
                                 cur->soft_limit_bytes >> 20,
                                 cur->hard_limit_bytes >> 20,
                                 cur->exit_code,
                                 cur->exit_signal);
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    (void)write_all(fd, buffer, strnlen(buffer, sizeof(buffer)));
}

static void stream_logs(supervisor_ctx_t *ctx, int fd, const char *container_id)
{
    container_record_t *container;
    int log_fd;
    char chunk[1024];
    ssize_t read_rc;

    pthread_mutex_lock(&ctx->metadata_lock);
    container = find_container_by_id(ctx, container_id);
    pthread_mutex_unlock(&ctx->metadata_lock);
    if (!container)
        return;

    log_fd = open(container->log_path, O_RDONLY);
    if (log_fd < 0)
        return;

    while ((read_rc = read(log_fd, chunk, sizeof(chunk))) > 0)
        (void)write_all(fd, chunk, (size_t)read_rc);

    close(log_fd);
}

static void stop_all_running_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (!cur->finished) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void cleanup_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        container_record_t *next = cur->next;

        if (cur->wait_for_exit)
            close_quietly(&cur->client_fd);
        close_quietly(&cur->log_read_fd);
        free(cur->child_stack);
        free(cur);
        cur = next;
    }

    ctx->containers = NULL;
}

static void handle_client_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    container_record_t *container;

    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));

    if (read_full(client_fd, &req, sizeof(req)) != 0) {
        close(client_fd);
        return;
    }

    switch (req.kind) {
    case CMD_START:
        if (start_container(ctx, &req, client_fd, 0, &resp) == 0)
            (void)write_all(client_fd, &resp, sizeof(resp));
        else
            (void)write_all(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;

    case CMD_RUN:
        if (start_container(ctx, &req, client_fd, 1, &resp) != 0) {
            (void)write_all(client_fd, &resp, sizeof(resp));
            close(client_fd);
        }
        break;

    case CMD_PS:
        (void)send_response_fd(client_fd, 0, "tracked containers");
        write_ps_listing(ctx, client_fd);
        close(client_fd);
        break;

    case CMD_LOGS:
        pthread_mutex_lock(&ctx->metadata_lock);
        container = find_container_by_id(ctx, req.container_id);
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!container) {
            (void)send_response_fd(client_fd, 1, "unknown container '%s'", req.container_id);
            close(client_fd);
            break;
        }
        (void)send_response_fd(client_fd, 0, "logs for %s", req.container_id);
        stream_logs(ctx, client_fd, req.container_id);
        close(client_fd);
        break;

    case CMD_STOP:
        pthread_mutex_lock(&ctx->metadata_lock);
        container = find_container_by_id(ctx, req.container_id);
        if (!container) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            (void)send_response_fd(client_fd, 1, "unknown container '%s'", req.container_id);
            close(client_fd);
            break;
        }
        container->stop_requested = 1;
        if (!container->finished)
            kill(container->host_pid, SIGTERM);
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)send_response_fd(client_fd, 0, "stop requested for %s", req.container_id);
        close(client_fd);
        break;

    default:
        (void)send_response_fd(client_fd, 1, "unsupported command");
        close(client_fd);
        break;
    }
}

static int open_monitor_device(void)
{
    int fd = open("/dev/container_monitor", O_RDWR);

    if (fd < 0)
        fprintf(stderr,
                "warning: could not open /dev/container_monitor (%s); continuing without kernel monitor\n",
                strerror(errno));
    return fd;
}

static int create_control_socket(void)
{
    int server_fd;
    struct sockaddr_un addr;

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    unlink(CONTROL_PATH);
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, 16) < 0) {
        close(server_fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return server_fd;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    copy_string(ctx.base_rootfs, sizeof(ctx.base_rootfs), rootfs);

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (install_signal_handlers() < 0) {
        perror("sigaction");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (ensure_log_dir() < 0) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open_monitor_device();
    ctx.server_fd = create_control_socket();
    if (ctx.server_fd < 0) {
        perror("create control socket");
        close_quietly(&ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        close_quietly(&ctx.server_fd);
        unlink(CONTROL_PATH);
        close_quietly(&ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    printf("supervisor listening on %s using base rootfs %s\n", CONTROL_PATH, ctx.base_rootfs);
    fflush(stdout);

    while (!ctx.should_stop) {
        fd_set readfds;
        struct timeval tv;
        int ready;

        if (g_supervisor_stop)
            ctx.should_stop = 1;

        reap_children(&ctx);

        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        ready = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (ready > 0 && FD_ISSET(ctx.server_fd, &readfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0)
                handle_client_request(&ctx, client_fd);
        }
    }

    stop_all_running_containers(&ctx);
    while (active_container_count(&ctx) > 0) {
        reap_children(&ctx);
        usleep(100000);
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    cleanup_containers(&ctx);
    close_quietly(&ctx.server_fd);
    unlink(CONTROL_PATH);
    close_quietly(&ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    char buffer[1024];
    ssize_t read_rc;
    int exit_status;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_all(fd, req, sizeof(*req)) != 0) {
        perror("write request");
        close(fd);
        return 1;
    }

    if (read_full(fd, &resp, sizeof(resp)) != 0) {
        fprintf(stderr, "failed to read supervisor response\n");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    while ((read_rc = read(fd, buffer, sizeof(buffer))) > 0)
        fwrite(buffer, 1, (size_t)read_rc, stdout);

    close(fd);

    exit_status = resp.status;
    if (req->kind != CMD_RUN && exit_status == 0)
        return 0;
    return exit_status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
