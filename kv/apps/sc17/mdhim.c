#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "mdhim.h"
#include "timer.h"

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)

int rank, size;
char* key_set;
size_t keylen;
size_t vallen;
size_t count;
int update_ratio;
int seed;
size_t key_index_off;
size_t key_index;
size_t i;

void rand_str(size_t len, char* str) {
    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int l = (int) (sizeof(charset) -1);
    for (int i = 0; i < len - 1; i++) {
        int key = rand() % l;
        str[i] = charset[key];
    }
    str[len - 1] = 0;
}

char* get_key(int idx) {
    return key_set + (idx * (keylen + 1));
}

void generate_key_set() {
    int off = 0;
    key_set = (char*) malloc((keylen + 1) * count);
    for (size_t i = 0; i < count; i++) {
        rand_str(keylen, get_key(i));
    }
}

int main(int argc, char **argv) {
	int ret;
	int provided = 0;
	struct mdhim_t *md;
	struct mdhim_brm_t *brm;
	struct mdhim_bgetrm_t *bgrm;
	char     *db_path = getenv("PAPYRUSKV_REPOSITORY");
	mdhim_options_t *db_opts; // Local variable for db create options to be passed
	MPI_Comm comm;

    if (argc < 5) {
        printf("[%s:%d] usage: %s keylen vallen count update_ratio[0:100]\n", __FILE__, __LINE__, argv[0]);
        return 0;
    }

    keylen = atol(argv[1]);
    vallen = atol(argv[2]);
    count = atol(argv[3]);
    update_ratio = atoi(argv[4]);

	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (ret != MPI_SUCCESS) {
		printf("Error initializing MPI with threads\n");
		exit(1);
	}

	if (provided != MPI_THREAD_MULTIPLE) {
                printf("Not able to enable MPI_THREAD_MULTIPLE mode\n");
                exit(1);
        }


	// Create options for DB initialization
	db_opts = mdhim_options_init();
	mdhim_options_set_db_path(db_opts, db_path);
	mdhim_options_set_db_name(db_opts, "sc17");
	mdhim_options_set_db_type(db_opts, LEVELDB);
	mdhim_options_set_key_type(db_opts, MDHIM_BYTE_KEY);
	mdhim_options_set_debug_level(db_opts, MLOG_CRIT);
    mdhim_options_set_max_recs_per_slice(db_opts, count);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    key_index_off = count * rank;
    key_index = 0;

    if (rank == 0) {
        double cb = (keylen + vallen) * count;
        double total = (keylen + vallen) * count * size;
        printf("[%s:%d] update_ratio[%d%%] keylen[%lu] vallen[%lu] count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB nranks[%d] total count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB\n", __FILE__, __LINE__, update_ratio, keylen, vallen, count, cb / KILO, cb / MEGA, cb / GIGA, size, count * size, total / KILO, total / MEGA, total / GIGA);
    }

	comm = MPI_COMM_WORLD;
	md = mdhimInit(&comm, db_opts);
	if (!md) {
		printf("Error initializing MDHIM\n");
		exit(1);
	}	

    char* key;
    char* put_val = (char*) malloc(vallen);
    size_t err = 0UL;
    size_t cnt_put = 0;
    size_t cnt_get = 0;

    snprintf(put_val, vallen, "THIS IS (%lu)B VAL FROM %d", vallen, rank);

    seed = rank * 17;
    srand(seed);
    generate_key_set();

    MPI_Barrier(MPI_COMM_WORLD);

	//Put the keys and values
    _w(0);
    for (i = 0; i < count; i++) {
        key = get_key(i);
        brm = mdhimPut(md, key, keylen, put_val, vallen, NULL, NULL);
        if (!brm || brm->error) printf("Error inserting key/value into MDHIM\n");
#ifdef VERBOSE
        if (i % 100 == 0) printf("[%s:%d] rank[%d] put[%lu]\n", __FILE__, __LINE__, rank, i);
#endif
    }
    //Commit the database
	mdhim_full_release_msg(brm);
    ret = mdhimCommit(md, md->primary_index);
    if (ret != MDHIM_SUCCESS) {
        printf("Error committing MDHIM database\n");
    }
    _w(1);

	MPI_Barrier(MPI_COMM_WORLD);

    _w(2);
    for (size_t i = 0; i < count; i++) {
        key = get_key(i);
        if (i % 100 < update_ratio) {
            brm = mdhimPut(md, key, keylen, put_val, vallen, NULL, NULL);
            if (!brm || brm->error) printf("Error inserting key/value into MDHIM\n");
            cnt_put++;
        } else {
            bgrm = mdhimGet(md, md->primary_index, key, keylen, MDHIM_GET_EQ);
            if (!bgrm || bgrm->error) {
                printf("Error getting value for key: %s from MDHIM\n", key);
                err++;
            }
            cnt_get++;
        }
#ifdef VERBOSE
        if (i % 100 == 0) printf("[%s:%d] rank[%d] pgt[%lu]\n", __FILE__, __LINE__, rank, i);
#endif
    }
    _w(3);

	ret = mdhimClose(md);
	if (ret != MDHIM_SUCCESS) {
		printf("Error closing MDHIM\n");
	}

    double time_put = _ww(0, 1);
    double time_pgt = _ww(2, 3);

    double time[2] = { time_put, time_pgt };

    double time_min[2];
    double time_max[2];
    double time_sum[2];

    if (err > 0) printf("[%s:%d] rank[%d] err[%lu]\n", __FILE__, __LINE__, rank, err);

    MPI_Reduce(time, time_min, 2, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(time, time_max, 2, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(time, time_sum, 2, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("[%s:%d] ALL[%lu] PUT[%lu] GET[%lu] AVG[PUT,PGT] [%lf,%lf] "
               "[MIN,MAX,AVG] PUT[%lf,%lf,%lf], PGT[%lf,%lf,%lf]\n",
                __FILE__, __LINE__, cnt_put + cnt_get, cnt_put, cnt_get,
                time_sum[0] / size, time_sum[1] / size,
                time_min[0], time_max[0], time_sum[0] / size,
                time_min[1], time_max[1], time_sum[1] / size);
    }

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
