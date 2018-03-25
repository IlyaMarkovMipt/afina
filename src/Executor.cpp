#include <iostream>
#include "afina/Executor.h"

namespace Afina {
    void perform(Executor &e) {
        std::function<void()> task;
        while (e.state.load() == Executor::State::kRun) {
            {
                e.threads.insert(std::this_thread::get_id());
                std::unique_lock<std::mutex> __lock(e.mutex);
                bool timed_out = false;
                e.sleeping_threads++;
                if (!e.empty_condition.wait_for(__lock, e.idle_time,
                                                 [&e]()->bool{ return !e.tasks.empty()
                                                                     || e.state.load() != Executor::State::kRun;})){
                    // timeout
                    if (e.threads.size() == e.low_watermark) {
                        // should not delete thread if there are too few threads
                        continue;
                    }
                    timed_out = true;
                }
                e.sleeping_threads--;
                if (timed_out || e.state.load() != Executor::State::kRun) {
                    goto clean;
                }

                task = std::move(e.tasks.front());
                e.tasks.pop_front();
            }
            task();
        }
    clean:
        e.threads.erase(std::this_thread::get_id());
        if (e.threads.empty()) {
            e.finish_condition.notify_one();
        }
    }

    Executor::Executor(int q_size, int high_wm,
                       int low_wm, int idle_timeout):
        Q_size(q_size), high_watermark(high_wm), low_watermark(low_wm),
        idle_time(idle_timeout * sec), sleeping_threads(0)
    {
        if (low_watermark > high_watermark) {
            std::cerr << __PRETTY_FUNCTION__ << "bad params" << std::endl;
            return;
        }
        std::unique_lock<std::mutex> __lock(mutex);
        for (int i = 0; i < low_wm; i++) {
            std::thread thread(perform, std::ref(*this));
            thread.detach();
        }
        state.store(State::kRun);
    }

    Executor::~Executor() {
        if (state.load() != State::kStopped) {
            Stop(true);
        }
    }

    void Executor::Stop(bool await) {
        state.store(State::kStopping);
        std::unique_lock<std::mutex>lock(mutex);
        empty_condition.notify_all();
        tasks.clear();
        sleeping_threads = 0;
        if (!await) {
            state.store(State::kStopped);
            return;
        }
        while (!threads.empty()) {
            finish_condition.wait(lock);
        }
        state.store(State::kStopped);
    }
}