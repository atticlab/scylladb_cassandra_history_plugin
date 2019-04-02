#ifndef CASS_THREAD_POOL_H
#define CASS_THREAD_POOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

namespace tp
{

    class ThreadPool {
    public:
        ThreadPool(size_t);
        void enqueue(std::function<void()>&& f);
        size_t queue_size() { return task_queue_size; }
        ~ThreadPool();
    private:
        // need to keep track of threads so we can join them
        std::vector< std::thread > workers;
        // the task queue
        std::queue< std::function<void()> > tasks;
        size_t task_queue_size;
        
        // synchronization
        std::mutex queue_mutex;
        std::condition_variable condition;
        bool stop;
    };
    
    // the constructor just launches some amount of workers
    inline ThreadPool::ThreadPool(size_t threads)
        :task_queue_size(0), stop(false)
    {
        for(size_t i = 0;i<threads;++i)
            workers.emplace_back(
                [this]
                {
                    for(;;)
                    {
                        std::function<void()> task;

                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            while ( !this->stop && this->tasks.empty()) {
                                this->condition.wait(lock);
                            }
                            if(this->stop && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                            task_queue_size--;
                        }

                        task();
                    }
                }
            );
    }

    // add new work item to the pool
    void ThreadPool::enqueue(std::function<void()>&& f)
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);

            // don't allow enqueueing after stopping the pool
            if(stop)
                throw std::runtime_error("enqueue on stopped ThreadPool");

            tasks.emplace(f);
            task_queue_size++;
        }
        condition.notify_one();
    }

    // the destructor joins all threads
    inline ThreadPool::~ThreadPool()
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for(std::thread &worker: workers)
            worker.join();
    }

}

#endif
