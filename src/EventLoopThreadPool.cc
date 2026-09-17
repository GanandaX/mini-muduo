#include "EventLoopThreadPool.h"
#include <assert.h>
#include <iostream>

EventLoopThreadPool::EventLoopThreadPool(EventLoop *baseLoop, size_t threadNum)
    : started_(false), baseLoop_(baseLoop), threadNum_(threadNum), next_(0) {
  assert(baseLoop_ != nullptr);
}

void EventLoopThreadPool::start() {
  assert(!started_);
  assert(baseLoop_->isInLoopThread());

  started_ = true;

  for (size_t i = 0; i < threadNum_; i++) {
    auto thread = std::make_unique<EventLoopThread>();
    auto loop = thread->startLoop();

    loops_.push_back(loop);
    threads_.push_back(std::move(thread));
  }
}

EventLoop *EventLoopThreadPool::getNextLoop() {
  assert(started_);
  assert(baseLoop_->isInLoopThread());

  if (!threadNum_) {
    return baseLoop_;
  }

  next_ %= threadNum_;
  return loops_[next_++];
}