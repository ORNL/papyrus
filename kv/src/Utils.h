#ifndef PAPYRUS_KV_SRC_UTILS_H
#define PAPYRUS_KV_SRC_UTILS_H

#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

namespace papyruskv {

class Utils {
public: 
static int Mkdir(const char* path) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);
    if (tmp[len - 1] == '/') tmp[len - 1] = 0;
    for (char* p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            if (access(tmp, F_OK) == -1) mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    if (access(tmp, F_OK) == -1) return mkdir(tmp, S_IRWXU);
    return 0;
}

static int MkdirRank(const char* path, int rank) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "%s/%d", path, rank);
    size_t len = strlen(tmp);
    if (tmp[len - 1] == '/') tmp[len - 1] = 0;
    for (char* p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            if (access(tmp, F_OK) == -1) mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
    if (access(tmp, F_OK) == -1) return mkdir(tmp, S_IRWXU);
    return 0;
}

static int Rmdir(const char* path) {
    DIR* d = opendir(path);
    int r = -1;
    if (d) {
        struct dirent* p;
        r = 0;
        while (!r && (p=readdir(d))) {
            int r2 = -1;
            if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
                continue;
            size_t len = strlen(path) + strlen(p->d_name) + 2; 
            char* buf = new char[len];
            struct stat statbuf;
            snprintf(buf, len, "%s/%s", path, p->d_name);
            if (!stat(buf, &statbuf)) {
                if (S_ISDIR(statbuf.st_mode)) r2 = Rmdir(buf);
                else r2 = unlink(buf);
            }
            delete[] buf;
            r = r2;
        }
        closedir(d);
    }
    if (!r) r = rmdir(path);
    return r;
}

static size_t P2(size_t size) {
    size_t p2 = 1;
    while (p2 < size) p2 = p2 << 1;
    return p2;
}

};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_UTILS_H */
