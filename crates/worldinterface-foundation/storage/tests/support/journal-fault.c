/* Linux-only test interposer. Stop inside SQLite's actual journal syscalls so the
 * parent can SIGKILL at boundaries that repository transaction hooks cannot see. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int journal_fd = -1;
static void stop_at_boundary(void) {
    int (*real_open)(const char *, int, ...) = dlsym(RTLD_NEXT, "open64");
    int ready = real_open(getenv("WI_CRASH_READY"), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (ready < 0 || write(ready, "ready", 5) != 5) _exit(90);
    close(ready);
    raise(SIGSTOP);
    _exit(91); /* The test parent must kill, never resume, this process. */
}
int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, int);
        va_end(ap);
    }
    int (*real_open)(const char *, int, ...) = dlsym(RTLD_NEXT, "open64");
    int fd = real_open(path, flags, mode);
    if (fd >= 0 && (flags & O_CREAT) && strstr(path, "fabric.sqlite-journal")) {
        journal_fd = fd;
        if (strcmp(getenv("WI_CRASH_POINT"), "journal-created") == 0) stop_at_boundary();
    }
    return fd;
}
ssize_t pwrite64(int fd, const void *buffer, size_t count, off64_t offset) {
    ssize_t (*real_pwrite)(int, const void *, size_t, off64_t) = dlsym(RTLD_NEXT, "pwrite64");
    if (fd == journal_fd && offset == 0) {
        const char *point = getenv("WI_CRASH_POINT");
        const char *prefix = "journal-header-";
        if (point && strncmp(point, prefix, strlen(prefix)) == 0) {
            size_t partial = strtoul(point + strlen(prefix), NULL, 10);
            if (partial == 0 || partial >= count) _exit(92);
            if (real_pwrite(fd, buffer, partial, offset) != (ssize_t)partial) _exit(93);
            if (fsync(fd) != 0) _exit(94);
            stop_at_boundary();
        }
    }
    return real_pwrite(fd, buffer, count, offset);
}
