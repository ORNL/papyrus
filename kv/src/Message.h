#ifndef PAPYRUS_KV_SRC_MESSAGE_H
#define PAPYRUS_KV_SRC_MESSAGE_H

#include <mpi.h>
#include <stdint.h>
#include <stdlib.h>

namespace papyruskv {

#define PAPYRUSKV_MSG_SIZE      0x100

#define PAPYRUSKV_MSG_TAG       0x0

#define PAPYRUSKV_MSG_PUT       0x2101
#define PAPYRUSKV_MSG_GET       0x2102
#define PAPYRUSKV_MSG_DELETE    0x2103
#define PAPYRUSKV_MSG_MIGRATE   0x2104
#define PAPYRUSKV_MSG_FLUSH     0x2105
#define PAPYRUSKV_MSG_SIGNAL    0x2106
#define PAPYRUSKV_MSG_BARRIER   0x2107
#define PAPYRUSKV_MSG_UPDATE    0x210c
#define PAPYRUSKV_MSG_EXIT      0x21ff

class Message {
public:
    Message(int header = -1);
    ~Message();

    void WriteHeader(int32_t v);
    void WriteBool(bool v);
    void WriteInt(int32_t v);
    void WriteUInt(uint32_t v);
    void WriteLong(int64_t v);
    void WriteULong(uint64_t v);
    void WriteFloat(float v);
    void WriteDouble(double v);
    void WriteString(const char* v);
    void WritePtr(void *ptr);
    void Write(const void* v, size_t size);

    int32_t ReadHeader();
    bool ReadBool();
    int32_t ReadInt();
    uint32_t ReadUInt();
    int64_t ReadLong();
    uint64_t ReadULong();
    float ReadFloat();
    double ReadDouble();
    char* ReadString();
    char* ReadString(size_t len);
    void* ReadPtr();
    void* Read(size_t size);

    void Send(int rank, MPI_Comm comm);
    void Ssend(int rank, MPI_Comm comm);
    int Recv(int rank, MPI_Comm comm);

    void Clear();

private:
    char buf_[PAPYRUSKV_MSG_SIZE] __attribute__ ((aligned(0x10)));
    size_t offset_;
    MPI_Status status_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_MESSAGE_H */
