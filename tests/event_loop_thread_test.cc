#include "EventLoop.h"
#include "EventLoopThread.h"
#include <assert.h>
#include <future>

void old_test() {
  std::promise<EventLoop *> loopPromise;
  std::future<EventLoop *> loopFuture = loopPromise.get_future();

  std::thread loopThread([&]() {
    EventLoop loop;
    loopPromise.set_value(&loop);
    loop.loop();
  });

  EventLoop *loop = loopFuture.get();
  bool executed = false;
  loop->queueInLoop([&]() {
    assert(loop->isInLoopThread());
    executed = true;
    loop->quit();
  });

  loopThread.join();
  assert(executed);
}

void event_loop_thread_test() {

  std::promise<void> done;
  auto doneFuture = done.get_future();

  {
    EventLoopThread loopThread;
    EventLoop *loop = loopThread.startLoop();

    assert(loop != nullptr);
    assert(!loop->isInLoopThread());

    loop->queueInLoop([&] {
      assert(loop->isInLoopThread());

      done.set_value();
    });
    doneFuture.get();
  }
}

int main() {
  event_loop_thread_test();
  return 0;
}