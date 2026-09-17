#include "EventLoopThreadPool.h"
#include "assert.h"
#include <future>

int main() {
  EventLoop baseLoop;

  EventLoopThreadPool loopPool(&baseLoop, 2);
  loopPool.start();

  EventLoop *loop0 = loopPool.getNextLoop();
  EventLoop *loop1 = loopPool.getNextLoop();

  assert(loop0 != loop1);
  assert(loopPool.getNextLoop() == loop0);
  assert(loopPool.getNextLoop() == loop1);
  assert(loopPool.getNextLoop() == loop0);

  std::vector<std::future<void>> futures;

  for (size_t i = 0; i < 5; i++) {
    auto loop = loopPool.getNextLoop();

    auto done = std::make_shared<std::promise<void>>();
    futures.push_back(done->get_future());

    loop->queueInLoop([loop, done]() {
      assert(loop->isInLoopThread());
      done->set_value();
    });
  }

  for (auto &future : futures) {
    future.get();
  }
}