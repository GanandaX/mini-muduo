#include "TimerQueue.h"
#include "EventLoop.h"
#include <assert.h>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <vector>

namespace {
TimerQueue::TimePoint now() { return std::chrono::steady_clock::now(); }

timespec toTimespec(TimerQueue::TimePoint time) {
  const auto duration = time.time_since_epoch();
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(duration);
  const auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);

  return timespec{static_cast<time_t>(seconds.count()),
                  static_cast<long>(nanoseconds.count())};
}

} // namespace

TimerQueue::TimerQueue(EventLoop *loop)
    : loop_(loop),
      timerfd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)),
      timerChannel_(timerfd_), callingExpiredTimers_(false) {
  assert(loop != nullptr);
  if (timerfd_ < 0) {
    const int savedErrno = errno;
    std::cerr << "timer fd create error: " << strerror(savedErrno) << '\n';
    std::abort();
  }

  timerChannel_.setReadCallback(std::bind(&TimerQueue::handleRead, this));
  timerChannel_.enableReading();
  loop_->updateChannel(&timerChannel_);
}

TimerQueue::~TimerQueue() {
  timerChannel_.disableAll();
  loop_->updateChannel(&timerChannel_);

  if (timerfd_ >= 0) {
    ::close(timerfd_);
    timerfd_ = -1;
  }
}

TimerId TimerQueue::addTimer(TimerId id, TimePoint when, TimerCallback cb,
                             std::chrono::milliseconds interval) {
  loop_->assertInLoopThread();

  bool earliestChanged = timers_.empty() || timers_.begin()->first > when;

  auto iter = timers_.emplace(when, Timer{id, std::move(cb), interval});
  activeTimers_.emplace(id, iter);

  if (earliestChanged) {
    resetTimerfd();
  }

  return id;
}

void TimerQueue::cancel(TimerId id) {
  loop_->assertInLoopThread();

  auto activeItr = activeTimers_.find(id);

  if (activeItr != activeTimers_.end()) {
    const bool earliestChanged = activeItr->second == timers_.begin();

    timers_.erase(activeItr->second);
    activeTimers_.erase(activeItr);

    if (earliestChanged) {
      resetTimerfd();
    }
  } else if (callingExpiredTimers_) {
    cancelingTimers_.insert(id);
  }
}

void TimerQueue::handleRead() {
  loop_->assertInLoopThread();

  uint64_t expirations = 0;
  ssize_t n;

  do {
    n = ::read(timerfd_, &expirations, sizeof(expirations));
  } while (n < 0 && errno == EINTR);

  if (n < 0 && errno == EAGAIN) {
    return;
  }

  if (n != static_cast<ssize_t>(sizeof(expirations))) {
    const int savedErrno = errno;
    std::cerr << "timerfd read error: " << strerror(savedErrno) << '\n';
    std::abort();
  }

  const TimePoint current = now();
  auto end = timers_.upper_bound(current);

  std::vector<Timer> expired;

  for (auto it = timers_.begin(); it != end; ++it) {
    activeTimers_.erase(it->second.id);
    expired.emplace_back(std::move(it->second));
  }

  timers_.erase(timers_.begin(), end);

  callingExpiredTimers_ = true;
  cancelingTimers_.clear();

  for (auto &timer : expired) {
    const TimerId id = timer.id;

    timer.callback();

    if (timer.repeat() && cancelingTimers_.find(id) == cancelingTimers_.end()) {
      auto timerItr = timers_.emplace(now() + timer.interval, std::move(timer));
      activeTimers_.emplace(id, timerItr);
    }
  }

  callingExpiredTimers_ = false;
  cancelingTimers_.clear();

  resetTimerfd();
}

void TimerQueue::resetTimerfd() {
  loop_->assertInLoopThread();
  itimerspec newValue{};

  if (!timers_.empty()) {
    newValue.it_value = toTimespec(timers_.begin()->first);
  }

  int ret = ::timerfd_settime(timerfd_, TFD_TIMER_ABSTIME, &newValue, nullptr);

  if (ret < 0) {
    const int savedErrno = errno;
    std::cerr << "timerfd_settime error: " << strerror(savedErrno) << '\n';
    std::abort();
  }
}
