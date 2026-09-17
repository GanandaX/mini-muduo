#include "EventLoopThread.h"
#include "InetAddress.h"
#include "TcpClient.h"
#include "TcpServer.h"

#include <assert.h>
#include <atomic>
#include <future>
#include <iostream>
#include <memory>
#include <string.h>
#include <sys/socket.h>

void graceful_stop_drains_connection_before_deadline_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);

  assert(::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                serverAddrLen) == 0);

  assert(::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen) == 0);

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::atomic<bool> stopRequested = false;

  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);
  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;

      if (clientConnCnt == 1) {
        if (!stopRequested.exchange(true)) {
          server->stop(std::chrono::seconds(5));
        }
      }
    } else {
      ++serverDisConnCnt;
    }
  });
  server->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++serverPeerCloseCnt;
    conn->shutdown();
  });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });
  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(500));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;

      if (serverConnCnt == 1) {
        if (!stopRequested.exchange(true)) {
          server->stop(std::chrono::seconds(5));
        }
      }
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connecte error" << std::endl;
    std::abort();
  });

  client.connect();
  TimerId stopTimeoutTimerId =
      loop.runAfter(std::chrono::milliseconds(1000), [&]() {
        std::cerr << "stop test timeout" << std::endl;
        std::abort();
      });
  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  loop.cancel(stopTimeoutTimerId);

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 1);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientPeerCloseCnt == 1);
}

void stop_forces_unresponsive_client_after_grace_period_test() {
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

  std::atomic<bool> stopRequested = false;
  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();
  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  TcpConnectionPtr clientConnection;
  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      clientConnection = conn;
      if ((serverConnCnt == 1) && !stopRequested.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });

  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });

  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++clientPeerCloseCnt; });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      if ((clientConnCnt == 1) && !stopRequested.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    if (clientConnection) {
      loop.queueInLoop([conn = clientConnection]() { conn->forceClose(); });
    }
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientPeerCloseCnt == 1);
}

void paused_acceptor_defers_pending_connection_until_resumed_test() {
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

  int serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  int clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStoppedAcceptPromise;
  auto serverStoppedAcceptFuture = serverStoppedAcceptPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;

      threadLoop->runAfter(std::chrono::milliseconds(100), [&]() {
        assert(serverConnCnt == 0);
        server->startAccepting();
      });
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });

  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      threadLoop->queueInLoop(
          [&]() { server->stop(std::chrono::milliseconds(1000)); });
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++serverPeerCloseCnt;
    conn->shutdown();
  });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  threadLoop->queueInLoop([&]() {
    server->stopAccepting();
    serverStoppedAcceptPromise.set_value();
  });

  waitForRes =
      serverStoppedAcceptFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop accepting timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 1);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientPeerCloseCnt == 1);
}

void stop_without_connections_completes_immediately_test() {
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

  int serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;

  EventLoop loop;
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(&loop, serverFd);

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++serverPeerCloseCnt;
    conn->shutdown();
  });

  server->setStopCompleteCallback([&]() { ++serverStopCompleteCnt; });

  server->start();
  server->stop(std::chrono::seconds(5));

  assert(serverConnCnt == 0);
  assert(serverDisConnCnt == 0);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
}

void stop_with_zero_grace_forces_active_connection_test() {
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

  std::atomic<bool> stopRequested = false;
  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStoppedCompletePromise;
  auto serverStoppedCompleteFuture = serverStoppedCompletePromise.get_future();

  TcpConnectionPtr clientConnection;

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      clientConnection = conn;
      ++clientConnCnt;

      if ((serverConnCnt == 1) && !stopRequested.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(0)); });
      }
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });

  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });

  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++clientPeerCloseCnt; });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;

      if ((clientConnCnt == 1) && !stopRequested.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(0)); });
      }
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    loop.queueInLoop([conn = clientConnection]() { conn->forceClose(); });
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStoppedCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(5000), [&]() {
    std::cerr << "watchdog timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStoppedCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
}

void start_after_stop_does_not_reopen_listener_test() {
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

  int serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  int clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientErrorCnt = 0;

  std::promise<void> restartAttemptedPromise;
  auto restartAttemptedFuture = restartAttemptedPromise.get_future();

  std::promise<void> serverDestroyedPromise;
  auto serverDestroyedFuture = serverDestroyedPromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      assert(false && "unexpected connection");
    } else {
      ++clientDisConnCnt;
    }
  });

  client.setConnectionErrorCallback([&](int error) {
    // std::cerr << "client connect error, is  ECONNREFUSED: "
    //           << ((error == ECONNREFUSED) ? "true" : "false") << std::endl;
    ++clientErrorCnt;
    assert(error == ECONNREFUSED);
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverDestroyedPromise.set_value();
    });
    loop.quit();
  });

  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++clientPeerCloseCnt; });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server->start();
      restartAttemptedPromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    server->stop(std::chrono::milliseconds(100));
  });

  auto waitForRes =
      restartAttemptedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server restart attempt timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(1000), [&]() {
    std::cerr << "server destroy timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes = serverDestroyedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 0);
  assert(serverDisConnCnt == 0);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 0);
  assert(clientDisConnCnt == 0);
  assert(clientErrorCnt == 1);
  assert(clientPeerCloseCnt == 0);
}

void repeated_stop_during_grace_period_is_ignored_test() {
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

  std::atomic<bool> stopRequired = false;
  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientErrorCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  std::promise<void> serverStopAssertedPromise;
  auto serverStopAssertedFuture = serverStopAssertedPromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  TcpConnectionPtr clientConn;
  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      clientConn = conn;
      if (serverConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });

  client.setConnectionErrorCallback([&](int error) { ++clientErrorCnt; });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    threadLoop->queueInLoop([&]() {
      server->stop(std::chrono::milliseconds(0));
      threadLoop->runAfter(std::chrono::milliseconds(100), [&]() {
        assert(serverStopCompleteCnt == 0);
        serverStopAssertedPromise.set_value();
      });
    });
  });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      if (clientConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    loop.queueInLoop([conn = clientConn]() { conn->forceClose(); });
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(2000), [&]() {
    std::cerr << "server destroy timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopAssertedFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop assert timeout" << std::endl;
    std::abort();
  }

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientErrorCnt == 0);
  assert(clientPeerCloseCnt == 1);
}

void stop_before_start_is_noop_and_later_start_succeeds_test() {
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

  std::atomic<bool> stopRequired = false;
  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientErrorCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      if (serverConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(5000)); });
      }
    } else {
      ++clientDisConnCnt;
      loop.quit();
    }
  });

  client.setConnectionErrorCallback([&](int) {
    ++clientErrorCnt;
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      if (clientConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(5000)); });
      }
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->stop(std::chrono::milliseconds(0));
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(2000), [&]() {
    std::cerr << "server test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 1);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientErrorCnt == 0);
  assert(clientPeerCloseCnt == 1);
}

void start_during_graceful_stop_does_not_resume_accepting_test() {
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

  std::atomic<bool> stopRequired = false;
  std::atomic<bool> loopQuitRequired = false;
  std::atomic<int> serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  std::atomic<int> clientConnCnt = 0;
  int clientDisConnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientErrorCnt = 0;
  std::atomic<int> probClientConnCnt = 0;
  int probClientDisConnCnt = 0;
  int probClientPeerCloseCnt = 0;
  int probClientErrorCnt = 0;

  std::promise<void> serverStartedPromise;
  auto serverStartedFuture = serverStartedPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  TcpConnectionPtr clientConn;
  EventLoop loop;
  TcpClient client(&loop, serverAddr);
  TcpClient probClient(&loop, serverAddr);

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      clientConn = conn;
      if (serverConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++clientDisConnCnt;
      if (probClientErrorCnt == 1 && !loopQuitRequired.exchange(true)) {
        loop.quit();
      }
    }
  });
  probClient.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++probClientConnCnt;
    } else {
      ++probClientDisConnCnt;
    }
  });

  client.setConnectionErrorCallback([&](int) {
    ++clientErrorCnt;
    std::abort();
  });
  probClient.setConnectionErrorCallback([&](int error) {
    ++probClientErrorCnt;
    assert(error == ECONNREFUSED);

    if (clientDisConnCnt == 1 && !loopQuitRequired.exchange(true)) {
      loop.quit();
    }
  });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;

    threadLoop->queueInLoop([&] {
      server->start();
      loop.queueInLoop([&]() { probClient.connect(); });
    });
  });
  probClient.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++probClientPeerCloseCnt; });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      if (clientConnCnt == 1 && !stopRequired.exchange(true)) {
        threadLoop->queueInLoop(
            [&]() { server->stop(std::chrono::milliseconds(1000)); });
      }
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    loop.queueInLoop([&]() { clientConn->forceClose(); });
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromise.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  client.connect();

  loop.runAfter(std::chrono::milliseconds(2000), [&]() {
    std::cerr << "server test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisConnCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisConnCnt == 1);
  assert(clientErrorCnt == 0);
  assert(clientPeerCloseCnt == 1);
  assert(probClientConnCnt == 0);
  assert(probClientDisConnCnt == 0);
  assert(probClientErrorCnt == 1);
  assert(probClientPeerCloseCnt == 0);
}

void stop_closes_paused_acceptor_and_refuses_new_connections_test() {
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

  int serverConnCnt = 0;
  int serverDisConnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  int probClientConnCnt = 0;
  int probClientDisConnCnt = 0;
  int probClientPeerCloseCnt = 0;
  int probClientErrorCnt = 0;

  std::promise<void> serverStoppedPromise;
  auto serverStoppedFuture = serverStoppedPromise.get_future();

  std::promise<void> serverStopCompletePromise;
  auto serverStopCompleteFuture = serverStopCompletePromise.get_future();

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();
  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd);

  EventLoop loop;
  TcpClient probClient(&loop, serverAddr);

  probClient.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++probClientConnCnt;
    } else {
      ++probClientDisConnCnt;
    }
  });

  probClient.setConnectionErrorCallback([&](int error) {
    ++probClientErrorCnt;
    assert(error == ECONNREFUSED);
    loop.quit();
  });

  probClient.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++probClientPeerCloseCnt; });

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
    } else {
      ++serverDisConnCnt;
    }
  });

  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++serverPeerCloseCnt; });

  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromise.set_value();
    });
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    server->stopAccepting();
    server->stop(std::chrono::milliseconds(0));
    serverStoppedPromise.set_value();
  });

  auto waitForRes =
      serverStoppedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }

  probClient.connect();

  loop.runAfter(std::chrono::milliseconds(2000), [&]() {
    std::cerr << "server test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 0);
  assert(serverDisConnCnt == 0);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(probClientConnCnt == 0);
  assert(probClientDisConnCnt == 0);
  assert(probClientErrorCnt == 1);
  assert(probClientPeerCloseCnt == 0);
}

void tcp_server_dispatches_connection_io_to_worker_loop_test() {
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

  char msg[] = "hello world!";
  ssize_t msgLen = strlen(msg);

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();
  std::promise<void> serverStopCompletePromose;
  auto serverStopCompleteFuture = serverStopCompletePromose.get_future();

  int serverConnCnt = 0;
  int serverDisconnCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;
  int serverMessageCnt = 0;

  int clientConnCnt = 0;
  int clientDisconnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientMessageCnt = 0;

  EventLoopThread threadEventLoop;
  auto threadLoop = threadEventLoop.startLoop();

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd, 1);
  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnCnt;
      assert(conn->loop() != threadLoop);
      assert(conn->loop()->isInLoopThread());
      assert(!threadLoop->isInLoopThread());
    } else {
      ++serverDisconnCnt;
    }
  });
  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromose.set_value();
    });
  });
  server->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++serverMessageCnt;
    assert(conn->loop() != threadLoop);
    assert(conn->loop()->isInLoopThread());
    assert(!threadLoop->isInLoopThread());
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
    threadLoop->queueInLoop(
        [&]() { server->stop(std::chrono::milliseconds(1000)); });
  });

  EventLoop loop;
  TcpClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      conn->send(msg, msgLen);
    } else {
      ++clientDisconnCnt;
      loop.quit();
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });
  client.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++clientMessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    buffer->retrieve(buffer->readableBytes());
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), [&]() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 1);
  assert(serverDisconnCnt == 1);
  assert(serverPeerCloseCnt == 1);
  assert(serverStopCompleteCnt == 1);
  assert(serverMessageCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisconnCnt == 1);
  assert(clientPeerCloseCnt == 1);
  assert(clientMessageCnt == 1);
}

void tcp_server_round_robins_connections_across_worker_loops_test() {
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

  char msg[] = "hello world!";
  ssize_t msgLen = strlen(msg);

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();
  std::promise<void> serverStopCompletePromose;
  auto serverStopCompleteFuture = serverStopCompletePromose.get_future();

  std::mutex mutex;

  std::atomic<bool> assertRequest = false;
  std::atomic<bool> stopRequest = false;
  std::atomic<bool> clientLoopStopRequest = false;

  int serverConnCnt = 0;
  std::atomic<int> serverDisconnCnt = 0;
  std::atomic<int> serverPeerCloseCnt = 0;
  std::atomic<int> serverMessageCnt = 0;
  int serverStopCompleteCnt = 0;

  int client1ConnCnt = 0;
  int client1DisconnCnt = 0;
  int client1PeerCloseCnt = 0;
  int client1MessageCnt = 0;

  int client2ConnCnt = 0;
  int client2DisconnCnt = 0;
  int client2PeerCloseCnt = 0;
  int client2MessageCnt = 0;

  EventLoopThread threadEventLoop;
  auto threadLoop = threadEventLoop.startLoop();

  EventLoop *loop1;
  EventLoop *loop2;

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd, 2);
  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      {
        std::lock_guard<std::mutex> locker(mutex);
        ++serverConnCnt;
        if (serverConnCnt == 1) {
          loop1 = conn->loop();
        } else if (serverConnCnt == 2) {
          loop2 = conn->loop();
        }

        if (serverConnCnt == 2 && !assertRequest.exchange(true)) {
          assert(loop1 != threadLoop);
          assert(loop2 != threadLoop);
          assert(loop1 != loop2);
        }
      }
    } else {
      ++serverDisconnCnt;
    }
  });
  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromose.set_value();
    });
  });
  server->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++serverMessageCnt;

    assert(conn->loop() != threadLoop);
    assert(conn->loop()->isInLoopThread());
    assert(!threadLoop->isInLoopThread());

    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());

    if (serverMessageCnt == 2 && !stopRequest.exchange(true)) {
      threadLoop->queueInLoop(
          [&]() { server->stop(std::chrono::milliseconds(1000)); });
    }
  });

  EventLoop loop;
  TcpClient client1(&loop, serverAddr);
  TcpClient client2(&loop, serverAddr);
  client1.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++client1ConnCnt;
      conn->send(msg, msgLen);
    } else {
      ++client1DisconnCnt;

      if (client2DisconnCnt == 1 && !clientLoopStopRequest.exchange(true)) {
        loop.quit();
      }
    }
  });
  client1.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++client1PeerCloseCnt;
    conn->shutdown();
  });
  client1.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++client1MessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    buffer->retrieve(buffer->readableBytes());
  });
  client1.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  client2.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++client2ConnCnt;
      conn->send(msg, msgLen);
    } else {
      ++client2DisconnCnt;

      if (client1DisconnCnt == 1 && !clientLoopStopRequest.exchange(true)) {
        loop.quit();
      }
    }
  });
  client2.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++client2PeerCloseCnt;
    conn->shutdown();
  });
  client2.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++client2MessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    buffer->retrieve(buffer->readableBytes());
  });
  client2.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  threadLoop->queueInLoop([&]() {
    server->start();
    serverStartedPromose.set_value();
  });

  auto waitForRes =
      serverStartedFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server start timeout" << std::endl;
    std::abort();
  }
  client1.connect();
  client2.connect();

  loop.runAfter(std::chrono::milliseconds(3000), [&]() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop timeout" << std::endl;
    std::abort();
  }

  assert(serverConnCnt == 2);
  assert(serverDisconnCnt == 2);
  assert(serverPeerCloseCnt == 2);
  assert(serverStopCompleteCnt == 1);
  assert(serverMessageCnt == 2);
  assert(client1ConnCnt == 1);
  assert(client1DisconnCnt == 1);
  assert(client1PeerCloseCnt == 1);
  assert(client1MessageCnt == 1);
  assert(client2ConnCnt == 1);
  assert(client2DisconnCnt == 1);
  assert(client2PeerCloseCnt == 1);
  assert(client2MessageCnt == 1);
}

void idle_connection_is_closed_after_server_timeout_test() {
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

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> serverStopCompletePromose;
  auto serverStopCompleteFuture = serverStopCompletePromose.get_future();

  int serverConnectCnt = 0;
  int serverDisconnectCnt = 0;
  int serverPeerCloseCnt = 0;
  int serverStopCompleteCnt = 0;

  int clientConnCnt = 0;
  int clientDisconnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientMessageCnt = 0;

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd, 1);
  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnectCnt;
    } else {
      ++serverDisconnectCnt;
    }
  });
  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromose.set_value();
    });
  });
  server->setIdleTimeout(std::chrono::milliseconds(1000));

  EventLoop loop;
  TcpClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
    } else {
      ++clientDisconnCnt;
      server->stop(std::chrono::milliseconds(1000));
      loop.quit();
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });
  client.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++clientMessageCnt;
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  threadLoop->queueInLoop([&]() {
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
  loop.runAfter(std::chrono::milliseconds(3000), [&]() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop timeout" << std::endl;
    std::abort();
  }

  assert(serverConnectCnt == 1);
  assert(serverDisconnectCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisconnCnt == 1);
  assert(clientMessageCnt == 0);
}

/**
 * 测试第一次send刷新了默认的timer后，后续的timer是否生效
 */
void inbound_data_refreshes_server_idle_timeout_test() {
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

  char msg[] = "hello world";
  ssize_t msgLen = strlen(msg);

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> idleRefreshVerifiedPromise;
  auto idleRefreshVerifiedFuture = idleRefreshVerifiedPromise.get_future();

  std::promise<void> serverStopCompletePromose;
  auto serverStopCompleteFuture = serverStopCompletePromose.get_future();

  std::atomic<int> serverConnectCnt = 0;
  std::atomic<int> serverDisconnectCnt = 0;
  std::atomic<int> serverPeerCloseCnt = 0;
  std::atomic<int> serverMessageCnt = 0;
  int serverStopCompleteCnt = 0;

  int clientConnCnt = 0;
  int clientDisconnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientMessageCnt = 0;

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd, 1);
  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnectCnt;
    } else {
      ++serverDisconnectCnt;
    }
  });
  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });
  server->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++serverMessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
    threadLoop->runAfter(std::chrono::milliseconds(700), [&]() {
      assert(serverDisconnectCnt.load() == 0);
      idleRefreshVerifiedPromise.set_value();
    });
  });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromose.set_value();
    });
  });
  server->setIdleTimeout(std::chrono::milliseconds(1000));

  EventLoop loop;
  TcpClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      loop.runAfter(std::chrono::milliseconds(600), [&, clientConn = conn]() {
        clientConn->send(msg, msgLen);
      });
    } else {
      ++clientDisconnCnt;
      server->stop(std::chrono::milliseconds(1000));
      loop.quit();
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });
  client.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++clientMessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    buffer->retrieve(buffer->readableBytes());
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  threadLoop->queueInLoop([&]() {
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

  loop.runAfter(std::chrono::milliseconds(3000), [&]() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      idleRefreshVerifiedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "idle refresh error" << std::endl;
    std::abort();
  }

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop timeout" << std::endl;
    std::abort();
  }

  assert(serverConnectCnt == 1);
  assert(serverDisconnectCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(serverMessageCnt == 1);
  assert(clientConnCnt == 1);
  assert(clientDisconnCnt == 1);
  assert(clientMessageCnt == 1);
}

/**
 * 测试timer刷新后，后续的timer是否会继续刷新
 */
void subsequent_inbound_data_refreshes_server_idle_timeout_test() {
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

  char msg[] = "hello world";
  ssize_t msgLen = strlen(msg);

  std::promise<void> serverStartedPromose;
  auto serverStartedFuture = serverStartedPromose.get_future();

  std::promise<void> idleRefreshVerifiedPromise;
  auto idleRefreshVerifiedFuture = idleRefreshVerifiedPromise.get_future();

  std::promise<void> serverStopCompletePromose;
  auto serverStopCompleteFuture = serverStopCompletePromose.get_future();

  std::atomic<int> serverConnectCnt = 0;
  std::atomic<int> serverDisconnectCnt = 0;
  std::atomic<int> serverPeerCloseCnt = 0;
  std::atomic<int> serverMessageCnt = 0;
  int serverStopCompleteCnt = 0;

  int clientConnCnt = 0;
  int clientDisconnCnt = 0;
  int clientPeerCloseCnt = 0;
  int clientMessageCnt = 0;

  TcpConnectionPtr clientConn;

  EventLoop loop;
  TcpClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  std::unique_ptr<TcpServer> server =
      std::make_unique<TcpServer>(threadLoop, serverFd, 1);

  server->setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnectCnt;
    } else {
      ++serverDisconnectCnt;
    }
  });
  server->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &) { ++serverPeerCloseCnt; });
  server->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++serverMessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());

    if (serverMessageCnt == 1) {
      loop.runAfter(std::chrono::milliseconds(300), [&]() {
        assert(clientConn);
        clientConn->send(msg, msgLen);
      });
    } else if (serverMessageCnt == 2) {
      conn->loop()->runAfter(std::chrono::milliseconds(800), [&]() {
        assert(serverDisconnectCnt.load() == 0);
        idleRefreshVerifiedPromise.set_value();
      });
    }
  });
  server->setStopCompleteCallback([&]() {
    ++serverStopCompleteCnt;
    threadLoop->queueInLoop([&]() {
      server.reset();
      serverStopCompletePromose.set_value();
    });
  });
  server->setIdleTimeout(std::chrono::milliseconds(1000));

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnCnt;
      clientConn = conn;
      loop.runAfter(std::chrono::milliseconds(200),
                    [&]() { clientConn->send(msg, msgLen); });
    } else {
      ++clientDisconnCnt;
      server->stop(std::chrono::milliseconds(1000));
      loop.quit();
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerCloseCnt;
    conn->shutdown();
  });
  client.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++clientMessageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    buffer->retrieve(buffer->readableBytes());
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "connection error" << std::endl;
    std::abort();
  });

  threadLoop->queueInLoop([&]() {
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

  loop.runAfter(std::chrono::milliseconds(3000), [&]() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  waitForRes =
      idleRefreshVerifiedFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "idle refresh error" << std::endl;
    std::abort();
  }

  waitForRes =
      serverStopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "server stop timeout" << std::endl;
    std::abort();
  }

  assert(serverConnectCnt == 1);
  assert(serverDisconnectCnt == 1);
  assert(serverPeerCloseCnt == 0);
  assert(serverStopCompleteCnt == 1);
  assert(serverMessageCnt == 2);
  assert(clientConnCnt == 1);
  assert(clientDisconnCnt == 1);
  assert(clientMessageCnt == 2);
}

int main() {
  graceful_stop_drains_connection_before_deadline_test();
  stop_forces_unresponsive_client_after_grace_period_test();
  paused_acceptor_defers_pending_connection_until_resumed_test();
  stop_without_connections_completes_immediately_test();
  stop_with_zero_grace_forces_active_connection_test();
  start_after_stop_does_not_reopen_listener_test();
  repeated_stop_during_grace_period_is_ignored_test();
  stop_before_start_is_noop_and_later_start_succeeds_test();
  start_during_graceful_stop_does_not_resume_accepting_test();
  stop_closes_paused_acceptor_and_refuses_new_connections_test();
  tcp_server_dispatches_connection_io_to_worker_loop_test();
  tcp_server_round_robins_connections_across_worker_loops_test();
  idle_connection_is_closed_after_server_timeout_test();
  inbound_data_refreshes_server_idle_timeout_test();
  subsequent_inbound_data_refreshes_server_idle_timeout_test();
}