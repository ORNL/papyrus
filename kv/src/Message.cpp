#include "Message.h"
#include "Debug.h"
#include <string.h>
#include <unistd.h>

namespace papyruskv {

Message::Message(int header) {
    offset_ = 0;
    if (header >= 0) WriteHeader(header);
}

Message::~Message() {
}

void Message::WriteHeader(int32_t v) {
    WriteInt(v);
}

void Message::WriteBool(bool v) {
    Write(&v, sizeof(v));
}

void Message::WriteInt(int32_t v) {
    Write(&v, sizeof(v));
}

void Message::WriteUInt(uint32_t v) {
    Write(&v, sizeof(v));
}

void Message::WriteLong(int64_t v) {
    Write(&v, sizeof(v));
}

void Message::WriteULong(uint64_t v) {
    Write(&v, sizeof(v));
}

void Message::WriteFloat(float v) {
    Write(&v, sizeof(v));
}

void Message::WriteDouble(double v) {
    Write(&v, sizeof(v));
}

void Message::WriteString(const char* v) {
    Write(v, strlen(v) + 1);
}

void Message::WritePtr(void *ptr) {
    Write(reinterpret_cast<void *>(&ptr), sizeof(void *));
}

void Message::Write(const void* v, size_t size) {
    memcpy(buf_ + offset_, v, size);
    offset_ += size;
    if (offset_ >= PAPYRUSKV_MSG_SIZE) _error("message size is over %d (%lu)", PAPYRUSKV_MSG_SIZE, offset_);
}

int32_t Message::ReadHeader() {
    return ReadInt();
}

bool Message::ReadBool() {
    return *(bool*) Read(sizeof(bool));
}

int32_t Message::ReadInt() {
    return *(int32_t*) Read(sizeof(int32_t));
}

uint32_t Message::ReadUInt() {
    return *(uint32_t*) Read(sizeof(uint32_t));
}

int64_t Message::ReadLong() {
    return *(int64_t*) Read(sizeof(int64_t));
}

uint64_t Message::ReadULong() {
    return *(uint64_t*) Read(sizeof(uint64_t));
}

float Message::ReadFloat() {
    return *(float*) Read(sizeof(float));
}

double Message::ReadDouble() {
    return *(double*) Read(sizeof(double));
}

char* Message::ReadString() {
    return (char*) Read(strlen(buf_ + offset_) + 1);
}

char* Message::ReadString(size_t len) {
    return (char*) Read(len);
}

void* Message::ReadPtr() {
    return *((void **) Read(sizeof(void *)));
}

void* Message::Read(size_t size) {
    void* p = buf_ + offset_;
    offset_ += size;
    return p;
}

void Message::Send(int rank, MPI_Comm comm) {
    MPI_Send(buf_, PAPYRUSKV_MSG_SIZE, MPI_CHAR, rank, PAPYRUSKV_MSG_TAG, comm);
    offset_ = 0;
}

void Message::Ssend(int rank, MPI_Comm comm) {
    MPI_Ssend(buf_, PAPYRUSKV_MSG_SIZE, MPI_CHAR, rank, PAPYRUSKV_MSG_TAG, comm);
    offset_ = 0;
}

int Message::Recv(int rank, MPI_Comm comm) {
    MPI_Recv(buf_, PAPYRUSKV_MSG_SIZE, MPI_CHAR, rank, PAPYRUSKV_MSG_TAG, comm, &status_);
    offset_ = 0;
    return status_.MPI_SOURCE;
}

void Message::Clear() {
    offset_ = 0;
}

} /* namespace papyruskv */
