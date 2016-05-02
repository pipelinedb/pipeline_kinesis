/*-------------------------------------------------------------------------
 *
 * conc_queue.hpp
 *	  Concurrent Bounded Queue
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONC_QUEUE_HPP
#define CONC_QUEUE_HPP

#include <mutex>
#include <queue>

template<typename Data>
class concurrent_queue
{
  public:
	concurrent_queue(size_t max_items)
		:
		queue_(),
		mutex_(),
		cond_var_has_items(),
		cond_var_has_space(),
		max_(max_items)
	{
	}

    bool empty() const
    {
		std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

	// push item onto the queue, wait up to ms if q full
	bool push_with_timeout(Data const& data, int ms)
    {
		{
			std::unique_lock<std::mutex> lock(mutex_);
			auto timeout = std::chrono::milliseconds(ms);

			if (max_ == queue_.size())
			{
				cond_var_has_space.wait_for(lock, timeout);
			}

			if (max_ == queue_.size())
				return false;

			queue_.push(data);
		}

        cond_var_has_items.notify_one();
		return true;
    }

	// pop item onto the queue, wait up to ms for item if q empty
	bool pop_with_timeout(Data& popped_value, int ms)
    {
		{
			std::unique_lock<std::mutex> lock(mutex_);
			auto timeout = std::chrono::milliseconds(ms);

			if (queue_.empty())
			{
				cond_var_has_items.wait_for(lock, timeout);
			}

			if (queue_.empty())
				return false;

			popped_value = queue_.front();
			queue_.pop();
		}

		cond_var_has_space.notify_one();
		return true;
    }

  private:
    std::queue<Data> queue_;
    mutable std::mutex mutex_;
	std::condition_variable cond_var_has_items;
	std::condition_variable cond_var_has_space;
	size_t max_;
};

#endif
