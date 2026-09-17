#include "EventLoop.h"
#include "TimerQueue.h"

#include <assert.h>
#include <iostream>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

EventLoop::EventLoop()
    : looping_(false), quit_(false), threadId_(std::this_thread::get_id()),
      activeChannels_(128), wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      wakeupChannel_(wakeupFd_), callingPendingFunctors_(false) {
  assert(wakeupFd_ >= 0);

  wakeupChannel_.setReadCallback(std::bind(&EventLoop::handleWakeupRead, this));

  wakeupChannel_.enableReading();
  updateChannel(&wakeupChannel_);
  timerQueue_ = std::make_unique<TimerQueue>(this);
}

EventLoop::~EventLoop() {
  assertInLoopThread();
  assert(!looping_);

  timerQueue_.reset();

  wakeupChannel_.disableAll();
  updateChannel(&wakeupChannel_);

  if (wakeupFd_ >= 0) {
    ::close(wakeupFd_);
    wakeupFd_ = -1;
  }
}

void EventLoop::loop() {
  looping_ = true;
  while (!quit_) {
    activeChannels_.clear();
    poller_.poll(-1, &activeChannels_);

    for (auto channel : activeChannels_) {
      channel->handleEvent();
    }

    doPendingFunctors();
  }
  looping_ = false;
}

void EventLoop::quit() {
  quit_ = true;

  if (!isInLoopThread()) {
    wakeup();
  }
}

void EventLoop::updateChannel(Channel *channel) {
  poller_.updateChannel(channel);
}

void EventLoop::removeChannel(Channel *channel) {
  poller_.removeChannel(channel);
}

bool EventLoop::isInLoopThread() const {
  return threadId_ == std::this_thread::get_id();
}

void EventLoop::assertInLoopThread() const { assert(isInLoopThread()); }

void EventLoop::handleWakeupRead() {
  uint64_t count = 0;

  ssize_t n;
  do {
    n = ::read(wakeupFd_, &count, sizeof(count));
  } while (n < 0 && errno == EINTR);

  if (n != static_cast<ssize_t>(sizeof(count))) {
    std::cerr << "eventfd read error: " << strerror(errno) << std::endl;
  }
}

void EventLoop::runInLoop(Functor cb) {
  if (isInLoopThread()) {
    cb();
  } else {
    queueInLoop(std::move(cb));
  }
}

void EventLoop::queueInLoop(Functor cb) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pendingFunctors_.push_back(std::move(cb));
  }

  if (!isInLoopThread() || callingPendingFunctors_) {
    wakeup();
  }
}

void EventLoop::wakeup() {
  uint64_t one = 1;

  ssize_t n;
  do {
    n = ::write(wakeupFd_, &one, sizeof(one));
  } while (n < 0 && errno == EINTR);

  if (n != static_cast<ssize_t>(sizeof(one))) {
    std::cerr << "eventfd write error: " << strerror(errno) << std::endl;
  }
}

TimerId EventLoop::runAfter(std::chrono::milliseconds delay, Functor cb) {
  const TimerId id = nextTimerId_.fetch_add(1);
  const auto when = std::chrono::steady_clock::now() + delay;

  if (isInLoopThread()) {
    timerQueue_->addTimer(id, when, std::move(cb));
  } else {
    queueInLoop([this, id, when, cb]() mutable {
      timerQueue_->addTimer(id, when, std::move(cb));
    });
  }

  return id;
}

TimerId EventLoop::runEvery(std::chrono::milliseconds interval, Functor cb) {
  const TimerId id = nextTimerId_.fetch_add(1);
  const auto when = std::chrono::steady_clock::now() + interval;

  if (isInLoopThread()) {
    timerQueue_->addTimer(id, when, std::move(cb), interval);
  } else {
    queueInLoop([this, id, when, cb, interval]() mutable {
      timerQueue_->addTimer(id, when, std::move(cb), interval);
    });
  }

  return id;
}

void EventLoop::cancel(TimerId id) {
  if (isInLoopThread()) {
    timerQueue_->cancel(id);
  } else {
    queueInLoop([this, id] { timerQueue_->cancel(id); });
  }
}

void EventLoop::doPendingFunctors() {
  std::vector<Functor> functors;

  callingPendingFunctors_.store(true);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    functors.swap(pendingFunctors_);
  }

  for (const Functor &functor : functors) {
    functor();
  }

  callingPendingFunctors_.store(false);
}