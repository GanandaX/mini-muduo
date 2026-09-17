#ifndef MINI_MUDUO_EVENTLOOP_H
#define MINI_MUDUO_EVENTLOOP_H

#include "Channel.h"
#include "EpollPoller.h"
#include "TimerId.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

class TimerQueue;

class EventLoop {
public:
  using Functor = std::function<void()>;

  explicit EventLoop();

  ~EventLoop();

  void loop();

  void quit();

  void updateChannel(Channel *channel);

  void removeChannel(Channel *channel);

  bool isInLoopThread() const;

  void assertInLoopThread() const;

  void handleWakeupRead();

  void runInLoop(Functor cb);

  void queueInLoop(Functor cb);

  void wakeup();

  TimerId runAfter(std::chrono::milliseconds delay, Functor cb);

  TimerId runEvery(std::chrono::milliseconds interval, Functor cb);

  void cancel(TimerId id);

private:
  void doPendingFunctors();

private:
  bool looping_;
  std::atomic_bool quit_;
  std::atomic<TimerId> nextTimerId_{1};

  std::thread::id threadId_;

  EpollPoller poller_;
  std::unique_ptr<TimerQueue> timerQueue_;
  std::vector<Channel *> activeChannels_;

  int wakeupFd_;
  Channel wakeupChannel_;

  std::mutex mutex_;
  std::vector<Functor> pendingFunctors_;
  std::atomic_bool callingPendingFunctors_;
};

#endif
