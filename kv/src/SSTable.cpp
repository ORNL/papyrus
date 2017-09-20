#include "SSTable.h"
#include "DB.h"
#include "Platform.h"
#include "Debug.h"
#include "Slice.h"
#include "Utils.h"
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sendfile.h>

namespace papyruskv {

SSTable::SSTable(DB* db, int mode) {
    db_ = db;
    rank_ = db->rank();
    nranks_ = db->nranks();
    group_ = db->group();
    sid_ = 0ULL;
    mode_ = mode;
    bloom_ = db->platform()->bloom();
    enable_bloom_ = db->platform()->enable_bloom();
    pool_ = db->platform()->pool();
    sprintf(root_, "%s", db->platform()->repository());
    pthread_mutex_init(&mutex_, NULL);
}

SSTable::~SSTable() {
    pthread_mutex_destroy(&mutex_);
}

uint64_t SSTable::Flush(MemTable* mt) {
    pthread_mutex_lock(&mutex_);
    char path[256];

    uint64_t sid = mt->mid();

    GetIDXPath(0, rank_, sid, root_, path);
    int fd_idx = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd_idx == -1) _error("path[%s]", path);

    GetSSTPath(0, rank_, sid, root_, path);
    int fd_sst = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd_sst == -1) _error("path[%s]", path);

    uint64_t idx = 0;
    for (Slice* slice = mt->head(); slice; slice = slice->next()) {
        slice_idx_t si = (slice_idx_t) { idx, slice->keylen(), slice->tombstone() ? (uint8_t) 1 : (uint8_t) 0 };
        ssize_t ssret = write(fd_idx, &si, sizeof(si));
        if (ssret != sizeof(si)) _error("ret[%ld] size[%lu]", ssret, sizeof(si));
        _trace("idx[%lu] len[%lu] tombstone[%d]", si.idx, si.len, si.tombstone);

        size_t kvsize = slice->kvsize();
        _trace("fd[%d] path[%s] key[%s] keylen[%lu] val[%s] vallen[%lu] kvsize[%lu] tombstone[%d]", fd_sst, path, slice->key(), slice->keylen(), slice->val(), slice->vallen(), kvsize, slice->tombstone());
        ssret = write(fd_sst, slice->buf(), kvsize);
        if (ssret != kvsize) _error("ret[%ld] kvsize[%lu]", ssret, kvsize);

        idx += kvsize;
    }

    if (enable_bloom_) {
        GetBLMPath(0, rank_, sid, root_, path);
        int fd_blm = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
        if (fd_blm == -1) _error("path[%s]", path);
        size_t bitslen = bloom_->len();
        uint64_t* bits = bloom_->Bits(mt->head());
        ssize_t ssret = write(fd_blm, bits, sizeof(uint64_t) * bitslen);
        if (ssret != sizeof(uint64_t) * bitslen) _error("ret[%ld] size[%lu]", ssret, sizeof(uint64_t) * bitslen);
        delete[] bits;
        int iret = close(fd_blm);
        if (iret == -1) _error("ret[%d]", iret);
    }

    int iret = close(fd_idx);
    if (iret == -1) _error("ret[%d]", iret);
    iret = close(fd_sst);
    if (iret == -1) _error("ret[%d]", iret);

    sid_ = sid;
    pthread_mutex_unlock(&mutex_);

    return sid_;
}

int SSTable::Get(const char* key, size_t keylen, char** valp, size_t* vallenp) {
    pthread_mutex_lock(&mutex_);
    uint64_t sid = sid_;
    pthread_mutex_unlock(&mutex_);
    return Get(key, keylen, valp, vallenp, rank_, sid);
}

int SSTable::Get(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, int sid) {
    if (sid == 0) return PAPYRUSKV_SLICE_NOT_FOUND;
    int ret = PAPYRUSKV_SLICE_NOT_FOUND;
    //TODO: outer-loop for levels
    for (uint64_t i = sid; ret == PAPYRUSKV_SLICE_NOT_FOUND && i > 0; i--) {
        char path[256];

        if (enable_bloom_) {
            GetBLMPath(0, rank, i, root_, path);
            int fd_blm = open(path, O_RDONLY);
            if (fd_blm == -1) { _error("path[%s]", path);
            } else {
                off_t fd_blm_size = lseek(fd_blm, 0, SEEK_END);
                size_t bitslen = fd_blm_size / sizeof(uint64_t);
                uint64_t* bits = new uint64_t[bitslen];
                ssize_t ssret = pread(fd_blm, bits, fd_blm_size, 0);
                if (ssret != fd_blm_size) _error("ret[%ld] size[%ld", ssret, fd_blm_size);
                int iret = close(fd_blm);
                if (iret != 0) _error("fd[%d] ret[%d]", fd_blm, iret);
                bool maybe = bloom_->Maybe(key, keylen, bits, bitslen);
                delete[] bits;
                if (!maybe) continue;
            }
        }

        GetIDXPath(0, rank, i, root_, path);
        int fd_idx = open(path, O_RDONLY);
        if (fd_idx == -1) {
            _error("path[%s]", path);
            break;
        }
        GetSSTPath(0, rank, i, root_, path);
        int fd_sst = open(path, O_RDONLY);
        if (fd_sst == -1) {
            _error("path[%s]", path);
            break;
        }
        off_t fd_idx_size = lseek(fd_idx, 0, SEEK_END);
        off_t fd_sst_size = lseek(fd_sst, 0, SEEK_END);
        _trace("fd_idx_size[%lu] fd_sst_size[%lu]", fd_idx_size, fd_sst_size);
        size_t idx_cnt = fd_idx_size / sizeof(slice_idx_t);
        slice_idx_t* idxes = new slice_idx_t[idx_cnt];
        ssize_t ssret = pread(fd_idx, idxes, fd_idx_size, 0);
        if (ssret != fd_idx_size) _error("read[%lu] fd_idx_size[%lu]", ssret, fd_idx_size);

        if (mode_ == PAPYRUSKV_SSTABLE_SEQ) ret = GetSequential(key, keylen, valp, vallenp, rank, idxes, idx_cnt, fd_sst, fd_sst_size);
        else if (mode_ == PAPYRUSKV_SSTABLE_BIN) ret = GetBinary(key, keylen, valp, vallenp, rank, idxes, idx_cnt, fd_sst, fd_sst_size);

        delete[] idxes;

        int iret = close(fd_idx);
        if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);
        iret = close(fd_sst);
        if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);
    }

    return ret;
}

int SSTable::GetSequential(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, slice_idx_t* idxes, size_t idx_cnt, int fd_sst, off_t fd_sst_size) {
    int ret = PAPYRUSKV_SLICE_NOT_FOUND;
    char* k = new char[keylen];
    for (size_t j = 0; j < idx_cnt; j++) {
        uint64_t idx = idxes[j].idx;
        uint64_t len = idxes[j].len;
        uint8_t tombstone = idxes[j].tombstone;
        _trace("idx[%lu] len[%lu] tombstone[%d]", idx, len, tombstone);
        if (keylen != len) continue;

        ssize_t ssret = pread(fd_sst, k, len, idx);
        if (ssret != len) _error("ssret[%ld] len[%lu]", ssret, len);
        _trace("key[%s]", key);

        if (strncmp(k, key, keylen) == 0) {
            if (tombstone) {
                ret = PAPYRUSKV_SLICE_TOMBSTONE;
                break;
            }
            size_t vallen = (j == idx_cnt - 1) ?
                fd_sst_size - idx - len : idxes[j + 1].idx - idx - len;
            if (vallenp) *vallenp = vallen;
            if (valp) {
                if (*valp == NULL) *valp = pool_->AllocVal(vallen);
                ssize_t ssret = pread(fd_sst, *valp, vallen, idx + len); 
                if (ssret != vallen) _error("ssret[%ld] vallen[%lu]", ssret, vallen);
                _trace("val[%s] vallen[%lu]", *valp, vallen);
            }
            ret = PAPYRUSKV_SLICE_FOUND;
            break;
        } else if (strncmp(k, key, keylen) > 0) {
            ret = PAPYRUSKV_SLICE_NOT_FOUND;
            break;
        }
    }
    delete[] k;
    return ret;
}

int SSTable::GetBinary(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, slice_idx_t* idxes, size_t idx_cnt, int fd_sst, off_t fd_sst_size) {
    int ret = PAPYRUSKV_SLICE_NOT_FOUND;
    char* k = new char[keylen];
    size_t idx_min = 0;
    size_t idx_max = idx_cnt - 1;
    do {
        size_t j = (idx_min + idx_max) / 2;

        uint64_t idx = idxes[j].idx;
        uint64_t len = idxes[j].len;
        uint8_t tombstone = idxes[j].tombstone;
        _trace("idx_min[%lu] idx_max[%lu] j[%lu] idx[%lu] len[%lu] tombstone[%d]", idx_min, idx_max, j, idx, len, tombstone);

        ssize_t ssret = pread(fd_sst, k, len, idx);
        if (ssret != len) _error("ssret[%ld] len[%lu]", ssret, len);
        _trace("key[%s]", key);

        uint64_t min_len = keylen < len ? keylen : len;

        if (strncmp(k, key, min_len) == 0) {
            if (keylen == len) {
                if (tombstone) {
                    ret = PAPYRUSKV_SLICE_TOMBSTONE;
                    break;
                }
                size_t vallen = (j == idx_cnt - 1) ?
                    fd_sst_size - idx - len : idxes[j + 1].idx - idx - len;
                if (vallenp) *vallenp = vallen;
                if (valp) {
                    if (*valp == NULL) *valp = pool_->AllocVal(vallen);
                    ssize_t ssret = pread(fd_sst, *valp, vallen, idx + len); 
                    if (ssret != vallen) _error("ssret[%ld] vallen[%lu]", ssret, vallen);
                    _trace("val[%s] vallen[%lu]", *valp, vallen);
                }
                ret = PAPYRUSKV_SLICE_FOUND;
                break;
            } else if (keylen < len) {
                idx_max = j - 1;
                continue;
            } else if (keylen > len) {
                idx_min = j + 1;
                continue;
            }
        } else if (strncmp(k, key, min_len) < 0) {
            if (j == idx_cnt - 1) break;
            idx_min = j + 1;
            continue;
        } else if (strncmp(k, key, min_len) > 0) {
            if (j == 0) break;
            idx_max = j - 1;
            continue;
        }
    } while (idx_min <= idx_max);
    delete[] k;
    return ret;
}

bool SSTable::SendFile(uint64_t sid, const char* suffix, const char* dst) {
    bool ret = true;
    char path[256];
    GetPath(0, rank_, sid, root_, suffix, path);
    int fd_src = open(path, O_RDONLY);
    if (fd_src == -1) {
        _error("path[%s]", path);
        return false;
    }
    off_t fd_size = lseek(fd_src, 0, SEEK_END);
    _trace("fd_size[%ld]", fd_size);
    off_t oret = lseek(fd_src, 0, SEEK_SET);
    if (oret != 0) _error("oret[%ld]", oret);

    GetPathNoRank(0, rank_, sid, (char*) dst, suffix, path);
    int fd_dst = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd_dst == -1) {
        _error("path[%s]", path);
        return false;
    }

    ssize_t ssret = sendfile(fd_dst, fd_src, NULL, (size_t) fd_size);
    if (ssret != fd_size) {
        _error("ssret[%zd] fd_size[%ld]", ssret, fd_size);
        ret = false;
    }

    int iret = close(fd_src);
    if (iret == -1) _error("path[%s]", path);
    iret = close(fd_dst);
    if (iret == -1) _error("path[%s]", path);

    return ret;
}

uint64_t SSTable::SendFiles(uint64_t sid, const char* dst) {
    pthread_mutex_lock(&mutex_);
    Utils::Mkdir(dst);
    //TODO: outer-loop for levels
    for (uint64_t i = sid; i > 0; i--) {
        SendFile(i, "idx", dst);
        SendFile(i, "sst", dst);
        if (enable_bloom_) SendFile(i, "blm", dst);
    }
    pthread_mutex_unlock(&mutex_);
    return sid;
}

bool SSTable::RecvFile(uint64_t sid, const char* suffix, const char* src) {
    bool ret = true;
    char path[256];
    GetPathNoRank(0, rank_, sid, (char*) src, suffix, path);
    int fd_src = open(path, O_RDONLY);
    if (fd_src == -1) {
        _error("path[%s]", path);
        return false;
    }
    off_t fd_size = lseek(fd_src, 0, SEEK_END);
    _trace("fd_size[%ld]", fd_size);
    off_t oret = lseek(fd_src, 0, SEEK_SET);
    if (oret != 0) _error("oret[%ld]", oret);

    GetPath(0, rank_, sid, root_, suffix, path);
    int fd_dst = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd_dst == -1) {
        _error("path[%s]", path);
        return false;
    }

    ssize_t ssret = sendfile(fd_dst, fd_src, NULL, (size_t) fd_size);
    if (ssret != fd_size) _error("ssret[%zd] fd_size[%ld]", ssret, fd_size);

    int iret = close(fd_src);
    if (iret == -1) _error("path[%s]", path);
    iret = close(fd_dst);
    if (iret == -1) _error("path[%s]", path);

    return ret;
}

uint64_t SSTable::RecvFiles(uint64_t sid, const char* src) {
    pthread_mutex_lock(&mutex_);
    //TODO: outer-loop for levels
    for (uint64_t i = sid; i > 0; i--) {
        RecvFile(i, "idx", src);
        RecvFile(i, "sst", src);
        if (enable_bloom_) RecvFile(i, "blm", src);
    }
    sid_ = sid;
    pthread_mutex_unlock(&mutex_);
    return sid_;
}

uint64_t SSTable::DistributeFiles(uint64_t* sids, int size, const char* root) {
//    pthread_mutex_lock(&mutex_);
    uint64_t sid = 0UL;
    uint64_t seq = 0UL;

    char* key = new char[PAPYRUSKV_MAX_KEYLEN];
    char* val = new char[PAPYRUSKV_MAX_VALLEN];

    for (int rank = 0; rank < size; rank++) {
        for (uint64_t i = sids[rank]; i > 0; i--) {
            if (seq++ % nranks_ != rank_) continue;
            sid++;
            _trace("distribute rank[%d] sid[%lu] seq[%d] sid[%lu]", rank, i, seq, sid);

            char path[256];
            GetIDXPath(0, rank, i, (char*) root, path);
            int fd_idx = open(path, O_RDONLY);
            if (fd_idx == -1) {
                _error("fd[%d] path[%s]", fd_idx, path);
                break;
            }
            GetSSTPath(0, rank, i, (char*) root, path);
            int fd_sst = open(path, O_RDONLY);
            if (fd_sst == -1) {
                _error("fd[%d] path[%s]", fd_sst, path);
                break;
            }
            off_t fd_idx_size = lseek(fd_idx, 0, SEEK_END);
            off_t fd_sst_size = lseek(fd_sst, 0, SEEK_END);
            _trace("fd_idx_size[%lu] fd_sst_size[%lu]", fd_idx_size, fd_sst_size);
            size_t idx_cnt = fd_idx_size / sizeof(slice_idx_t);
            slice_idx_t* idxes = new slice_idx_t[idx_cnt];
            ssize_t ssret = pread(fd_idx, idxes, fd_idx_size, 0);
            if (ssret != fd_idx_size) _error("read[%zd] fd_idx_size[%lu]", ssret, fd_idx_size);

            for (size_t j = 0; j < idx_cnt; j++) {
                uint64_t idx = idxes[j].idx;
                uint64_t len = idxes[j].len;
                uint8_t tombstone = idxes[j].tombstone;
                _trace("idx[%lu] len[%lu] tombstone[%d]", idx, len, tombstone);

                ssize_t ssret = pread(fd_sst, key, len, idx);
                if (ssret != len) _error("ret[%zd] keylen[%lu]", ssret, len);
                _trace("key[%s] keylen[%lu]", key, len);

                size_t vallen = (j == idx_cnt - 1) ?
                    fd_sst_size - idx - len : idxes[j + 1].idx - idx - len;
                ssret = pread(fd_sst, val, vallen, idx + len); 
                if (ssret != vallen) _error("ret[%zd] vallen[%lu]", ssret, vallen);
                _trace("key[%s] keylen[%lu] val[%s] vallen[%lu]", key, len, val, vallen);

                int ret = db_->Put((const char*) key, len, (const char*) val, vallen);
                _trace("ret[%d] key[%s] keylen[%lu] val[%s] vallen[%lu]", ret, key, len, val, vallen);
                if (ret != PAPYRUSKV_OK) _error("ret[%d] key[%s] keylen[%lu] val[%s] vallen[%lu]", ret, key, len, val, vallen);
            }
            delete[] idxes;

            int iret = close(fd_idx);
            if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);
            iret = close(fd_sst);
            if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);
        }
    }
    delete[] key;
    delete[] val;
    delete[] sids;
//    pthread_mutex_unlock(&mutex_);
    return sid;
}

int SSTable::WriteTOC(uint64_t* sids, int size, const char* root) {
    Utils::Mkdir(root);

    char path[256];
    GetTOCPath(root, path);
    int fd_toc = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd_toc == -1) _error("path[%s]", path);

    ssize_t ssret = write(fd_toc, &size, sizeof(size));
    if (ssret != sizeof(size)) _error("ssret[%zd] size[%lu]", ssret, sizeof(size));

    ssret = write(fd_toc, sids, size * sizeof(uint64_t));
    if (ssret != size * sizeof(uint64_t)) _error("ssret[%zd] size[%lu]", ssret, size * sizeof(uint64_t));

    int ret = close(fd_toc);
    if (ret == -1) _error("ret[%d]", ret);

    return PAPYRUSKV_OK;
}

int SSTable::ReadTOC(uint64_t** sids, int* size, const char* root) {
    char path[256];
    GetTOCPath(root, path);
    int fd_toc = open(path, O_RDONLY);
    if (fd_toc == -1) _error("path[%s]", path);

    int nranks = 0;
    ssize_t ssret = read(fd_toc, &nranks, sizeof(int));
    if (ssret != sizeof(int)) _error("ssret[%zd] size[%lu] err[%s]", ssret, sizeof(int), strerror(errno));

    *size = nranks;
    *sids = new uint64_t[nranks];
    ssret = read(fd_toc, *sids, nranks * sizeof(uint64_t));
    if (ssret != nranks * sizeof(uint64_t)) _error("ssret[%zd] size[%lu]", ssret, nranks * sizeof(uint64_t));
    
    int ret = close(fd_toc);
    if (ret == -1) _error("ret[%d]", ret);

    return PAPYRUSKV_OK;
}

int SSTable::Load(MemTable* mt, uint64_t sid) {
    char* key = new char[PAPYRUSKV_MAX_KEYLEN];
    char* val = new char[PAPYRUSKV_MAX_VALLEN];

    char path[256];
    GetIDXPath(0, rank_, sid, root_, path);
    int fd_idx = open(path, O_RDONLY);
    if (fd_idx == -1) {
        _error("fd[%d] path[%s]", fd_idx, path);
        return PAPYRUSKV_ERR;
    }
    GetSSTPath(0, rank_, sid, root_, path);
    int fd_sst = open(path, O_RDONLY);
    if (fd_sst == -1) {
        _error("fd[%d] path[%s]", fd_sst, path);
        return PAPYRUSKV_ERR;
    }
    off_t fd_idx_size = lseek(fd_idx, 0, SEEK_END);
    off_t fd_sst_size = lseek(fd_sst, 0, SEEK_END);
    _trace("fd_idx_size[%lu] fd_sst_size[%lu]", fd_idx_size, fd_sst_size);
    size_t idx_cnt = fd_idx_size / sizeof(slice_idx_t);
    slice_idx_t* idxes = new slice_idx_t[idx_cnt];
    ssize_t ssret = pread(fd_idx, idxes, fd_idx_size, 0);
    if (ssret != fd_idx_size) _error("read[%zd] fd_idx_size[%lu]", ssret, fd_idx_size);

    for (size_t j = 0; j < idx_cnt; j++) {
        uint64_t idx = idxes[j].idx;
        uint64_t len = idxes[j].len;
        uint8_t tombstone = idxes[j].tombstone;
        _trace("idx[%lu] len[%lu] tombstone[%d]", idx, len, tombstone);

        ssize_t ssret = pread(fd_sst, key, len, idx);
        if (ssret != len) _error("ret[%zd] keylen[%lu]", ssret, len);
        _trace("key[%s] keylen[%lu]", key, len);

        size_t vallen = (j == idx_cnt - 1) ?
            fd_sst_size - idx - len : idxes[j + 1].idx - idx - len;
        ssret = pread(fd_sst, val, vallen, idx + len); 
        if (ssret != vallen) _error("ret[%zd] vallen[%lu]", ssret, vallen);
        _trace("key[%s] keylen[%lu] val[%s] vallen[%lu]", key, len, val, vallen);

        Slice* slice = new Slice(key, len, val, vallen, rank_, tombstone == 1);
        mt->Put(slice);
    }
    delete[] key;
    delete[] val;
    delete[] idxes;

    int iret = close(fd_idx);
    if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);
    iret = close(fd_sst);
    if (iret != 0) _error("fd[%d] ret[%d]", fd_idx, iret);

    return PAPYRUSKV_OK;
}

void SSTable::GetPath(int level, int rank, uint64_t sid, char* root, const char* suffix, char* path) {
    sprintf(path, "%s/%d/%s_%d_%d_%lu.%s", root, rank, db_->name(), rank, level, sid, suffix);
}

void SSTable::GetPathNoRank(int level, int rank, uint64_t sid, char* root, const char* suffix, char* path) {
    sprintf(path, "%s/%s_%d_%d_%lu.%s", root, db_->name(), rank, level, sid, suffix);
}

void SSTable::GetSSTPath(int level, int rank, uint64_t sid, char* root, char* path) {
    GetPath(level, rank, sid, root, "sst", path);
}

void SSTable::GetIDXPath(int level, int rank, uint64_t sid, char* root, char* path) {
    GetPath(level, rank, sid, root, "idx", path);
}

void SSTable::GetBLMPath(int level, int rank, uint64_t sid, char* root, char* path) {
    GetPath(level, rank, sid, root, "blm", path);
}

void SSTable::GetTOCPath(const char* root, char* path) {
    sprintf(path, "%s/%s.toc", root, db_->name());
}

} /* namespace papyruskv */
