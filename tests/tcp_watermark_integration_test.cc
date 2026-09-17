#include <assert.h>
#include <cstddef>
#include <future>
#include <iostream>
#include <memory>
#include <sys/socket.h>

#include "EventLoop.h"
#include "TcpClient.h"
#include "TcpServer.h"

void server_high_water_mark_callback_is_forwarded_test() {
  const size_t HIGH_WATER_MARK = 10;

  int highWaterCallbackCnt = 0;

  std::promise<void> highWaterCallbackPromise;
  auto highWaterCallbackFuture = highWaterCallbackPromise.get_future();

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                serverAddrLen));
  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen));

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();
  std::unique_ptr<TcpServer> server = std::make_unique<TcpServer>(
      loop, serverFd, 0,
      [writeCallCount = size_t{0}](int fd, const void *,
                                   size_t n) mutable -> ssize_t {
        switch (++writeCallCount) {
        case 1:
          errno = EAGAIN;
          return -1;

        default:
          return n;
        }
      });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      loop->queueInLoop([clientConn = conn]() {
        char buf[1024];
        clientConn->send(buf, HIGH_WATER_MARK - 1);
        clientConn->send(buf, 1);
      });
    }
  });

  server->setHighWaterMarkCallback(
      [&](const TcpConnectionPtr &, size_t bufferedBytes) {
        ++highWaterCallbackCnt;
        assert(highWaterCallbackCnt == 1);
        assert(bufferedBytes == HIGH_WATER_MARK);
        highWaterCallbackPromise.set_value();
      },
      HIGH_WATER_MARK);
  server->setLowWaterMarkCallback(nullptr, HIGH_WATER_MARK / 2);

  server->setWriteCompleteCallback([&](const TcpConnectionPtr &) {
    loop->queueInLoop([&]() { server->stop(std::chrono::milliseconds(0)); });
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server->setStopCompleteCallback([&]() {
    loop->queueInLoop([&]() {
      server.reset();
      stopCompletePromise.set_value();
    });
  });

  EventLoop clientLoop;
  TcpClient client(&clientLoop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (!conn->connected()) {
      clientLoop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connect error" << std::endl;
    std::abort();
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  loop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });
  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  clientLoop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  clientLoop.loop();

  waitForRes =
      highWaterCallbackFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "high water callback timeout" << std::endl;
    std::abort();
  }
  assert(highWaterCallbackCnt == 1);

  waitForRes = stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }
}

void server_low_water_mark_callback_is_forwarded_test() {
  const size_t LOW_WATER_MARK = 10;

  int lowWaterCallbackCnt = 0;
  std::promise<void> lowWaterCallbackPromise;
  auto lowWaterCallbackFuture = lowWaterCallbackPromise.get_future();

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                serverAddrLen));
  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen));

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();
  std::unique_ptr<TcpServer> server = std::make_unique<TcpServer>(
      loop, serverFd, 0,
      [writeCallCount = size_t{0}](int fd, const void *,
                                   size_t n) mutable -> ssize_t {
        switch (++writeCallCount) {
        case 1:
          errno = EAGAIN;
          return -1;
        case 2:
          return 1;
        case 3:
          errno = EAGAIN;
          return -1;
        default:
          return n;
        }
      });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      loop->queueInLoop([clientConn = conn]() {
        char buf[1024];
        clientConn->send(buf, LOW_WATER_MARK + 1);
      });
    }
  });

  server->setLowWaterMarkCallback(
      [&](const TcpConnectionPtr &, size_t bufferedBytes) {
        ++lowWaterCallbackCnt;
        assert(lowWaterCallbackCnt == 1);
        assert(bufferedBytes == LOW_WATER_MARK);
        lowWaterCallbackPromise.set_value();
      },
      LOW_WATER_MARK);
  server->setHighWaterMarkCallback(nullptr, LOW_WATER_MARK << 1);

  server->setWriteCompleteCallback([&](const TcpConnectionPtr &) {
    loop->queueInLoop([&]() { server->stop(std::chrono::milliseconds(0)); });
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server->setStopCompleteCallback([&]() {
    loop->queueInLoop([&]() {
      server.reset();
      stopCompletePromise.set_value();
    });
  });

  EventLoop clientLoop;
  TcpClient client(&clientLoop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (!conn->connected()) {
      clientLoop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connect error" << std::endl;
    std::abort();
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  loop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });
  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  clientLoop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  clientLoop.loop();

  waitForRes = lowWaterCallbackFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "low water callback timeout" << std::endl;
    std::abort();
  }
  assert(lowWaterCallbackCnt == 1);

  waitForRes = stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }
}

void client_high_water_mark_callback_is_forwarded_test() {
  const size_t HIGH_WATER_MARK = 10;

  int highWaterCallbackCnt = 0;
  std::promise<void> highWaterCallbackPromise;
  auto highWaterCallbackFuture = highWaterCallbackPromise.get_future();

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                serverAddrLen));
  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen));

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(loop, serverFd, 0);

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server->setStopCompleteCallback([&]() {
    loop->queueInLoop([&]() {
      server.reset();
      stopCompletePromise.set_value();
    });
  });

  EventLoop clientLoop;
  TcpClient client(&clientLoop, serverAddr,
                   [writeCallCount = size_t{0}](int fd, const void *,
                                                size_t n) mutable -> ssize_t {
                     switch (++writeCallCount) {
                     case 1:
                       errno = EAGAIN;
                       return -1;

                     default:
                       return n;
                     }
                   });
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      clientLoop.queueInLoop([clientConn = conn]() {
        char buf[1024];
        clientConn->send(buf, HIGH_WATER_MARK - 1);
        clientConn->send(buf, 1);
      });
    } else {
      clientLoop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connect error" << std::endl;
    std::abort();
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  client.setWriteCompleteCallback([&](const TcpConnectionPtr &conn) {
    conn->shutdown();
    loop->queueInLoop([&]() { server->stop(std::chrono::milliseconds(0)); });
  });

  client.setHighWaterMarkCallback(
      [&](const TcpConnectionPtr &, size_t bufferedBytes) {
        ++highWaterCallbackCnt;
        assert(highWaterCallbackCnt == 1);
        assert(bufferedBytes == HIGH_WATER_MARK);
        highWaterCallbackPromise.set_value();
      },
      HIGH_WATER_MARK);
  client.setLowWaterMarkCallback(nullptr, HIGH_WATER_MARK / 2);

  loop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });
  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  clientLoop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  clientLoop.loop();

  waitForRes =
      highWaterCallbackFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "high water callback timeout" << std::endl;
    std::abort();
  }
  assert(highWaterCallbackCnt == 1);

  waitForRes = stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }
}

void client_low_water_mark_callback_is_forwarded_test() {
  const size_t LOW_WATER_MARK = 10;

  int lowWaterCallbackCnt = 0;
  std::promise<void> lowWaterCallbackPromise;
  auto lowWaterCallbackFuture = lowWaterCallbackPromise.get_future();

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                serverAddrLen));
  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen));

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(loop, serverFd, 0);

  server->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    conn->shutdown();
    loop->queueInLoop([&]() { server->stop(std::chrono::milliseconds(0)); });
  });

  server->setStopCompleteCallback([&]() {
    loop->queueInLoop([&]() {
      server.reset();
      stopCompletePromise.set_value();
    });
  });

  EventLoop clientLoop;
  TcpClient client(&clientLoop, serverAddr,
                   [writeCallCount = size_t{0}](int fd, const void *,
                                                size_t n) mutable -> ssize_t {
                     switch (++writeCallCount) {
                     case 1:
                       errno = EAGAIN;
                       return -1;
                     case 2:
                       return 1;
                     case 3:
                       errno = EAGAIN;
                       return -1;
                     default:
                       return n;
                     }
                   });
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      clientLoop.queueInLoop([clientConn = conn]() {
        char buf[1024];
        clientConn->send(buf, LOW_WATER_MARK + 1);
      });
    } else {
      clientLoop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connect error" << std::endl;
    std::abort();
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  client.setWriteCompleteCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });

  client.setLowWaterMarkCallback(
      [&](const TcpConnectionPtr &, size_t bufferedBytes) {
        ++lowWaterCallbackCnt;
        assert(lowWaterCallbackCnt == 1);
        assert(bufferedBytes == LOW_WATER_MARK);
        lowWaterCallbackPromise.set_value();
      },
      LOW_WATER_MARK);
  client.setHighWaterMarkCallback(nullptr, LOW_WATER_MARK << 1);

  loop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });
  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  clientLoop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  clientLoop.loop();

  waitForRes = lowWaterCallbackFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "low water callback timeout" << std::endl;
    std::abort();
  }
  assert(lowWaterCallbackCnt == 1);

  waitForRes = stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }
}

int main() {
  server_high_water_mark_callback_is_forwarded_test();
  std::cout << "===================" << std::endl;
  server_low_water_mark_callback_is_forwarded_test();
  std::cout << "===================" << std::endl;
  client_high_water_mark_callback_is_forwarded_test();
  std::cout << "===================" << std::endl;
  client_low_water_mark_callback_is_forwarded_test();
}