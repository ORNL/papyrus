#ifndef PAPYRUS_KV_SRC_TIMER_H
#define PAPYRUS_KV_SRC_TIMER_H

#define PAPYRUSKV_TIMER_MAX 128

namespace papyruskv {

class Timer {
private:
    Timer();
    ~Timer();

public:
    double Now();
    void Start(int i);
    void Stop(int i);
    double Total(int i);

private:
    double total_[PAPYRUSKV_TIMER_MAX];
    double start_[PAPYRUSKV_TIMER_MAX];
    double base_sec_;

public:
    static Timer* GetTimer();

private:
    static Timer* singleton_;
};

} /* namespace papyruskv */

#endif /*PAPYRUS_KV_SRC_TIMER_H */
