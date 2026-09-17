#ifndef MINI_MUDUO_TIMER_QUEUE_H
#define MINI_MUDUO_TIMER_QUEUE_H

#include "Channel.h"
#include "TimerId.h"

#include <chrono>
#include <functional>
#include <map>
#include <unordered_map>
#include <unordered_set>

class EventLoop;

class TimerQueue {

public:
  using TimerCallback = std::function<void()>;
  using TimePoint = std::chrono::steady_clock::time_point;

  explicit TimerQueue(EventLoop *loop);
  ~TimerQueue();

  TimerId
  addTimer(TimerId id, TimePoint when, TimerCallback cb,
           std::chrono::milliseconds interval = std::chrono::milliseconds(0));

  void cancel(TimerId id);

private:
  struct Timer {
    TimerId id;
    TimerCallback callback;
    std::chrono::milliseconds interval;

    bool repeat() const { return interval.count() > 0; }
  };

private:
  using TimerMap = std::multimap<TimePoint, Timer>;
  using TimerIterator = TimerMap::iterator;

  void handleRead();
  void resetTimerfd();

private:
  EventLoop *loop_;
  int timerfd_;
  Channel timerChannel_;

  bool callingExpiredTimers_;
  // 在执行回调函数时删除的Timer
  std::unordered_set<TimerId> cancelingTimers_;

  TimerMap timers_;
  std::unordered_map<TimerId, TimerIterator> activeTimers_;
};

#endif