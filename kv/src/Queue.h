#ifndef PAPYRUS_KV_SRC_QUEUE_H
#define PAPYRUS_KV_SRC_QUEUE_H

namespace papyruskv {

// Single Producer & Single Consumer
template<typename T>
class LockFreeQueue {
public:
    LockFreeQueue(unsigned long size) {
        size_ = size;
        idx_r_ = 0;
        idx_w_ = 0;
        elements_ = (volatile T*)(new T[size_]);
    }

    virtual ~LockFreeQueue() {
        delete[] elements_;
    }

    virtual bool Enqueue(T element) {
        unsigned long next_idx_w = (idx_w_ + 1) % size_;
        if (next_idx_w == idx_r_) return false;
        elements_[idx_w_] = element;
        __sync_synchronize();
        idx_w_ = next_idx_w;
        return true;
    }

    bool Dequeue(T* element) {
        if (idx_r_ == idx_w_) return false;
        unsigned long next_idx_r = (idx_r_ + 1) % size_;
        *element = (T) elements_[idx_r_];
        idx_r_ = next_idx_r;
        return true;
    }

    bool Peek(T* element) {
        if (idx_r_ == idx_w_) return false;
        *element = (T) elements_[idx_r_];
        return true;
    }

    unsigned long Size() {
        if (idx_w_ >= idx_r_) return idx_w_ - idx_r_;
        return size_ - idx_r_ + idx_w_;
    }

    bool Empty() {
        return Size() == 0UL;
    }

protected:
    unsigned long size_;
    volatile T* elements_;
    volatile unsigned long idx_r_;
    volatile unsigned long idx_w_;
};

// Multiple Producers & Single Consumer
template<typename T>
class LockFreeQueueMS: public LockFreeQueue<T> {
public:
    LockFreeQueueMS(unsigned long size) : LockFreeQueue<T>::LockFreeQueue(size) {
        idx_w_cas_ = 0;
    }
    ~LockFreeQueueMS() {}

    bool Enqueue(T element) {
        while (true) {
            unsigned long prev_idx_w = idx_w_cas_;
            unsigned long next_idx_w = (prev_idx_w + 1) % this->size_;
            if (next_idx_w == this->idx_r_) return false;
            if (__sync_bool_compare_and_swap(&idx_w_cas_, prev_idx_w, next_idx_w)) {
                this->elements_[prev_idx_w] = element;
                while (!__sync_bool_compare_and_swap(&this->idx_w_, prev_idx_w, next_idx_w)) {}
                break;
            }
        }
        return true;
    }

private:
    volatile unsigned long idx_w_cas_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_QUEUE_H */
