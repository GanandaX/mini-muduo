#ifndef MINI_MUDUO_EVENT_LOOP_THREAD_POOL_H
#define MINI_MUDUO_EVENT_LOOP_THREAD_POOL_H
#include "EventLoop.h"
#include "EventLoopThread.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
#include <string>

class EventLoopThreadPool {
public:
  explicit EventLoopThreadPool(EventLoop *baseLoop, size_t threadNum);

  void start();
  EventLoop *getNextLoop();

private:
  bool started_;

  EventLoop *baseLoop_;
  size_t threadNum_;
  size_t next_;

  std::vector<EventLoop *> loops_;
  std::vector<std::unique_ptr<EventLoopThread>> threads_;
};

#endif