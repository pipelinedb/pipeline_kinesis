#include <mutex>
#include <queue>

template<typename Data>
class concurrent_queue
{
private:
    std::queue<Data> queue_;
    mutable std::mutex mutex_;
	std::condition_variable cond_var_has_items;
	std::condition_variable cond_var_has_space;
	size_t max_;
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

    void push(Data const& data)
    {
		{
			std::unique_lock<std::mutex> lock(mutex_);

			while (max_ == queue_.size())
			{
				cond_var_has_space.wait(lock);
			}

			queue_.push(data);
		}

        cond_var_has_items.notify_one();
    }

    bool empty() const
    {
		std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    bool try_pop(Data& popped_value)
    {
		{
			std::unique_lock<std::mutex> lock(mutex_);

			if(queue_.empty())
			{
				return false;
			}

			popped_value=queue_.front();
			queue_.pop();
		}

		cond_var_has_space.notify_one();
        return true;
    }

    void wait_and_pop(Data& popped_value)
    {
		{
			std::unique_lock<std::mutex> lock(mutex_);

			while(queue_.empty())
			{
				cond_var_has_items.wait(lock);
			}

			popped_value=queue_.front();
			queue_.pop();
		}

		cond_var_has_space.notify_one();
    }
};
