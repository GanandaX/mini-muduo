#ifndef MINI_MUDUO_EVENT_LOOP_THREAD_H
#define MINI_MUDUO_EVENT_LOOP_THREAD_H

#include <atomic>
#include <condition_variable>
#include <thread>

#include "EventLoop.h"

class EventLoopThread {
public:
  explicit EventLoopThread();
  ~EventLoopThread();

  EventLoop *startLoop();

private:
  void threadFunc();

private:
  bool started_;
  std::thread loopThread_;

  std::mutex mutex_;
  std::condition_variable cond_;
  EventLoop *loop_;
};

#endif