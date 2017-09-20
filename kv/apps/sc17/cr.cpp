#include <mpi.h>
#include <papyrus/kv.h>
#include <papyrus/mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "timer.h"

//#define VERBOSE

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)

int rank, size;
char name[256];
char* lustre;
int ret;
int db;
size_t keylen;
size_t vallen;
size_t count;
char cr;
int seed;

void rand_str(size_t len, char* str) {
    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int l = (int) (sizeof(charset) -1);
    for (int i = 0; i < len - 1; i++) {
        int key = rand() % l;
        str[i] = charset[key];
    }
    str[len - 1] = 0;
}

void checkpoint() {
    papyruskv_option_t opt;
    opt.keylen = keylen;
    opt.vallen = vallen;
    opt.hash = NULL;

    ret = papyruskv_open("mydb", PAPYRUSKV_CREATE | PAPYRUSKV_RELAXED | PAPYRUSKV_RDWR, &opt, &db);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);

    char* key = new char[keylen];
    char* input_val = new char[vallen];
    sprintf(input_val, "THIS IS (%lu)B VAL FROM %d", vallen, rank);

    seed = rank;
    srand(seed);

    _w(0);
    for (size_t i = 0; i < count; i++) {
        rand_str(keylen, key);
        ret = papyruskv_put(db, key, keylen, input_val, vallen);
        if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
#ifdef VERBOSE
        if (i % 1000 == 0) printf("[%s:%d] rank[%d] put[%lu]\n", __FILE__, __LINE__, rank, i);
#endif
    }
    _w(1);
    ret = papyruskv_barrier(db, PAPYRUSKV_SSTABLE);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    _w(2);
    ret = papyruskv_checkpoint(db, (const char*) lustre, NULL);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    _w(3);
    ret = papyruskv_close(db);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    _w(4);
}

void restart() {
    papyruskv_option_t opt;
    opt.keylen = keylen;
    opt.vallen = vallen;
    opt.hash = NULL;
    _w(0);
    _w(1);
    _w(2);
    ret = papyruskv_restart((const char*) lustre, "mydb", PAPYRUSKV_RDWR, &opt, &db, NULL);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    _w(3);
    ret = papyruskv_close(db);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    _w(4);
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    papyruskv_init(&argc, &argv, "$PAPYRUSKV_REPOSITORY");

    if (argc < 5) {
        printf("[%s:%d] usage: %s keylen vallen count lustre cr[c,r]\n", __FILE__, __LINE__, argv[0]);
        papyruskv_finalize();
        MPI_Finalize();
        return 0;
    }

    keylen = atol(argv[1]);
    vallen = atol(argv[2]);
    count = atol(argv[3]);
    lustre = argv[4];
    cr = argv[5][0];

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(name, &ret);

    if (rank == 0) {
        double cb = (keylen + vallen) * count;
        double total = (keylen + vallen) * count * size;
        printf("[%s:%d] cr[%c] keylen[%lu] vallen[%lu] count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB nranks[%d] total count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB target[%s]\n", __FILE__, __LINE__, cr, keylen, vallen, count, cb / KILO, cb / MEGA, cb / GIGA, size, count * size, total / KILO, total / MEGA, total / GIGA, lustre);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (cr == 'c') checkpoint();
    else restart();

    double time_put   = _ww(0, 1);
    double time_bar   = _ww(1, 2);
    double time_cr    = _ww(2, 3);
    double time_close = _ww(3, 4);

    double time[4] = { time_put, time_bar, time_cr, time_close };

    double time_min[4];
    double time_max[4];
    double time_sum[4];

    MPI_Reduce(time, time_min, 4, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(time, time_max, 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(time, time_sum, 4, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("[%s:%d] AVG[PUT,BAR,CR,CLOSE] [%lf,%lf,%lf,%lf]"
               "[MIN,MAX,AVG] PUT[%lf,%lf,%lf] BAR[%lf,%lf,%lf] CR[%lf,%lf,%lf] CLOSE[%lf,%lf,%lf]\n",
                __FILE__, __LINE__,
                time_sum[0] / size, time_sum[1] / size, time_sum[2] / size, time_sum[3] / size,
                time_min[0], time_max[0], time_sum[0] / size,
                time_min[1], time_max[1], time_sum[1] / size,
                time_min[2], time_max[2], time_sum[2] / size,
                time_min[3], time_max[3], time_sum[3] / size);
    }

    papyruskv_finalize();
    MPI_Finalize();

    return 0;
}

