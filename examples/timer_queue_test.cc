#include "EventLoop.h"
#include "TcpConnection.h"
#include "TcpServer.h"
#include "TimerQueue.h"

#include <assert.h>
#include <errno.h>
#include <iostream>
#include <memory>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

void test_1() {
  EventLoop loop;

  bool fired = false;
  const auto start = std::chrono::steady_clock::now();

  loop.runAfter(std::chrono::milliseconds(100), [&] {
    fired = true;

    const auto elapsed = std::chrono::steady_clock::now() - start;
    assert(elapsed >= std::chrono::milliseconds(100));

    loop.quit();
  });

  loop.loop();
  assert(fired);
}

void test_2() {

  EventLoop loop;
  std::vector<int> vec;

  loop.runAfter(std::chrono::milliseconds(200), [&] { vec.push_back(1); });
  loop.runAfter(std::chrono::milliseconds(50), [&] { vec.push_back(2); });
  loop.runAfter(std::chrono::milliseconds(250), [&] {
    vec.push_back(3);
    loop.quit();
  });

  loop.loop();
  assert((vec == std::vector<int>{2, 1, 3}));
}

void runEvery() {

  EventLoop loop;
  int counter = 0;
  loop.runEvery(std::chrono::milliseconds(1000), [&]() {
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count()
              << " runEvery" << std::endl;

    if (++counter >= 3) {
      loop.quit();
    }
  });

  loop.loop();

  assert(counter == 3);
}

void cancelTimer() {
  EventLoop loop;

  uint8_t counter = 0;

  TimerId tId_1 =
      loop.runAfter(std::chrono::milliseconds(200), [&]() { ++counter; });

  TimerId tId_2 =
      loop.runAfter(std::chrono::milliseconds(100), [&]() { ++counter; });

  TimerId tId_3 = loop.runAfter(std::chrono::milliseconds(300), [&]() {
    ++counter;
    loop.quit();
  });

  loop.cancel(tId_1);

  loop.loop();
  assert(counter == 2);
}

void cancelTimerInCallback() {
  EventLoop loop;

  TimerId id = 0;
  int count = 0;

  id = loop.runEvery(std::chrono::milliseconds(30), [&] {
    ++count;
    std::cout << "count : " << count << std::endl;

    if (count == 3) {
      loop.cancel(id);
    }
  });

  loop.runAfter(std::chrono::milliseconds(150), [&] { loop.quit(); });

  loop.loop();

  assert(count == 3);
}

void connection_timeout_test() {
  int fds[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                    fds) == 0);

  EventLoop loop;

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  bool closed = false;

  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    closed = true;
    loop.quit();
  });

  conn->connectEstablished();
  conn->setIdleTimeout(std::chrono::milliseconds(500));

  loop.loop();

  assert(closed);
  conn->connectDestroyed();
  close(fds[1]);
}

void connection_timeout_pingpong_test() {
  int fds[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                    fds) == 0);

  EventLoop loop;

  const auto start = std::chrono::steady_clock::now();
  const auto idleTimeout = std::chrono::milliseconds(200);

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  bool retrived = false;
  bool closed = false;

  conn->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    retrived = true;
    buffer->retrieve(buffer->readableBytes());
  });

  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    closed = true;
    loop.quit();
  });

  conn->connectEstablished();
  conn->setIdleTimeout(std::chrono::milliseconds(200));

  loop.runAfter(std::chrono::milliseconds(100), [&] {
    const char msg[] = "ping";
    assert(::write(fds[1], msg, sizeof(msg) - 1) == (sizeof(msg) - 1));
  });

  loop.loop();

  const auto elapse = std::chrono::steady_clock::now() - start;
  assert(elapse >= idleTimeout);
  std::cout
      << "elapse: "
      << std::chrono::duration_cast<std::chrono::milliseconds>(elapse).count()
      << std::endl;

  assert(closed);
  assert(retrived);
  conn->connectDestroyed();
  close(fds[1]);
}

void connection_five_seconds_timeout_test() {
  EventLoop baseLoop;

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
  assert(fd >= 0);

  int reuse = 1;
  assert(::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) ==
         0);

  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(2007);
  int ret = bind(fd, (sockaddr *)(&addr), sizeof(addr));
  if (ret < 0) {
    std::cout << "error: " << strerror(errno) << std::endl;
    return;
  }

  TcpServer server(&baseLoop, fd, 2);
  server.setIdleTimeout(std::chrono::seconds(5));

  server.start();

  baseLoop.loop();
}

int main() { connection_five_seconds_timeout_test(); }