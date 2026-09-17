#include "EventLoop.h"
#include "TcpConnection.h"
#include <assert.h>
#include <future>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "EventLoopThread.h"

void send_from_thread() {

  int fds[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                    fds) == 0);

  std::promise<TcpConnectionPtr> connPromise;
  auto connFuture = connPromise.get_future();
  std::promise<EventLoop *> eventLoopPromise;
  auto eventLoopFuture = eventLoopPromise.get_future();

  std::thread ioThread([&]() {
    EventLoop loop;
    TcpConnectionPtr connPtr = std::make_shared<TcpConnection>(&loop, fds[0]);

    connPtr->connectEstablished();

    connPromise.set_value(connPtr);
    eventLoopPromise.set_value(&loop);
    loop.loop();

    connPtr->connectDestroyed();
  });

  TcpConnectionPtr conn = connFuture.get();
  EventLoop *loop = eventLoopFuture.get();

  constexpr char kMessage[] = "cross_thread_send";
  constexpr size_t kMessageLen = sizeof(kMessage) - 1;

  conn->send(kMessage, kMessageLen);

  conn->loop()->queueInLoop([conn] { conn->loop()->quit(); });

  ioThread.join();

  char received[64] = {0};
  ssize_t n = read(fds[1], received, sizeof(received));

  assert(n == static_cast<ssize_t>(kMessageLen));
  assert(std::string(received, n) == kMessage);

  close(fds[1]);
}

void shutdown_from_thread() {

  int fds[2];
  assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                    fds) == 0);

  std::promise<TcpConnectionPtr> connPromise;
  auto connFuture = connPromise.get_future();

  std::thread ioThread([&]() {
    EventLoop loop;
    TcpConnectionPtr connPtr = std::make_shared<TcpConnection>(&loop, fds[0]);

    connPtr->connectEstablished();

    connPromise.set_value(connPtr);
    loop.loop();

    connPtr->connectDestroyed();
  });

  TcpConnectionPtr conn = connFuture.get();

  constexpr char kMessage[] = "cross_thread_send";
  constexpr size_t kMessageLen = sizeof(kMessage) - 1;

  conn->send(kMessage, kMessageLen);
  conn->shutdown();

  pollfd pfd{};
  pfd.fd = fds[1];
  pfd.events = POLLIN;

  assert(::poll(&pfd, 1, 1000) > 0);

  char received[64] = {0};
  ssize_t n = read(fds[1], received, sizeof(received));

  assert(n == static_cast<ssize_t>(kMessageLen));
  assert(std::string(received, n) == kMessage);

  n = ::read(fds[1], received, sizeof(received));
  assert(n == 0);

  conn->loop()->quit();
  close(fds[1]);
  ioThread.join();
}

void eventLoopThread_test() {
  EventLoopThread loopThread;
  EventLoop *loop = loopThread.startLoop();

  std::atomic_bool ran{false};
  loop->runInLoop([&]() {
    loop->assertInLoopThread();
    ran = true;
  });

  loop->queueInLoop([loop] { loop->quit(); });
}

int main() {
  // send_from_thread();
  //   shutdown_from_thread();
  eventLoopThread_test();
}