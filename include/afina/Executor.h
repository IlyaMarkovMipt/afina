#ifndef AFINA_THREADPOOL_H
#define AFINA_THREADPOOL_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <set>
#include <atomic>
#include <chrono>

namespace Afina {

class Executor;

void perform(Executor &executor);


/**
 * # Thread pool
 */
class Executor {
    const std::chrono::seconds sec = std::chrono::seconds(1);
    enum class State {
        // Threadpool is fully operational, tasks could be added and get executed
        kRun,

        // Threadpool is on the way to be shutdown, no ned task could be added, but existing will be
        // completed as requested
        kStopping,

        // Threadppol is stopped
        kStopped
    };

    // No copy/move/assign allowed
    Executor(const Executor &);            // = delete;
    Executor(Executor &&);                 // = delete;
    Executor &operator=(const Executor &); // = delete;
    Executor &operator=(Executor &&);      // = delete;

    /**
     * Main function that all pool threads are running. It polls internal task queue and execute tasks
     */
    friend void perform(Executor &executor);

    /**
     * Mutex to protect state below from concurrent modification
     */
    std::mutex mutex;

    /**
     * Conditional variable to await new data in case of empty queue
     */
    std::condition_variable empty_condition;

    /**
     * Conditional variable to await finish
     */
    std::condition_variable finish_condition;

    /**
     * Mutex to protect threads from concurrent modification
     */
    std::mutex threads_mutex;

    /**
     * Vector of actual threads that perform execution
     */
    std::set<std::thread::id> threads;
    /**
     * Task queue
     */
    std::deque<std::function<void()>> tasks;
    /**
     * Atomic flag to notify threads when it is time to stop. Note that
     * flag must be atomic in order to safely publish changes cross thread
     * bounds
     */
    std::atomic<State> state;
    /**
     * Max number of tasks. No new tasks are accepted if this threshold is reached
     */
    int Q_size;

    /**
     * Maximum number of threads
     */
    int high_watermark;

    /**
     * Minimum number of threads
     */
    int low_watermark;

    /**
     * Time thread waits waking in idle state
     */
    std::chrono::duration<int> idle_time;

    int sleeping_threads;

public:
    Executor(int q_size=10, int high_wm=5, int low_wm=2, int idle_time = 10);
    ~Executor();

    /**
     * Signal thread pool to stop, it will stop accepting new jobs and close threads just after each become
     * free. All enqueued jobs will be complete.
     *
     * In case if await flag is true, call won't return until all background jobs are done and all threads are stopped
     */
    void Stop(bool await = false);

    /**
     * Add function to be executed on the threadpool. Method returns true in case if task has been placed
     * onto execution queue, i.e scheduled for execution and false otherwise.
     *
     * That function doesn't wait for function result. Function could always be written in a way to notify caller about
     * execution finished by itself
     */
    template <typename F, typename... Types> bool Execute(F &&func, Types... args){
        // Prepare "task"
        auto exec = std::bind(std::forward<F>(func), std::forward<Types>(args)...);

        if (state.load() != State::kRun) {
            return false;
        }
        std::unique_lock<std::mutex> lock(this->mutex);
        if (tasks.size() >= Q_size) {
            return false;
        }
        if (sleeping_threads == 0 &&
            threads.size() < high_watermark) {
            std::thread *thread = new std::thread(perform, std::ref(*this));
            threads.insert(thread->get_id());
            thread->detach();
        }
        // Enqueue new task
        tasks.push_back(exec);
        empty_condition.notify_one();
        return true;
    }

};

} // namespace Afina

#endif // AFINA_THREADPOOL_H
