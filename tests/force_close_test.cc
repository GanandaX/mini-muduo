#include <assert.h>
#include <future>
#include <iostream>
#include <memory>
#include <sys/socket.h>
#include <unistd.h>

#include "LengthHeaderCodec.h"
#include "TcpClient.h"
#include "TcpConnection.h"
#include "TcpServer.h"

void force_close_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK,
                           0, fds));

  uint8_t connAConnected = 0;
  uint8_t connADisconnected = 0;
  uint8_t connAClosed = 0;
  uint8_t connBConnected = 0;
  uint8_t connBDisconnected = 0;
  uint8_t connBClosed = 0;

  EventLoop loop;
  auto connA = std::make_shared<TcpConnection>(&loop, fds[0]);
  auto connB = std::make_shared<TcpConnection>(&loop, fds[1]);

  connA->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connAConnected;
    } else {
      ++connADisconnected;
    }
  });
  connA->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++connAClosed;
    conn->connectDestroyed();
  });

  connB->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connBConnected;
    } else {
      ++connBDisconnected;
    }
  });
  connB->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++connBClosed;
    conn->connectDestroyed();
    loop.quit();
  });
  connB->setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  connA->connectEstablished();
  connB->connectEstablished();

  connA->forceClose();

  loop.runAfter(std::chrono::milliseconds(2000), [&] {
    std::cerr << "测试超时" << std::endl;
    std::abort();
  });
  loop.loop();

  assert(connAConnected == 1);
  assert(connADisconnected == 1);
  assert(connAClosed == 1);
  assert(connBConnected == 1);
  assert(connBDisconnected == 1);
  assert(connBClosed == 1);
}

void server_force_close_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  int reuse = 1;
  assert(0 == ::setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                           sizeof(reuse)));

  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddress.getSockAddr()),
                serverAddressLen));
  assert(0 == ::getsockname(
                  serverFd,
                  reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                  &serverAddressLen));

  int serverConnected = 0;
  int serverDisconnected = 0;
  int clientConnected = 0;
  int clientDisconnected = 0;
  int clientPeerHalfCloseCount = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddress);

  TcpConnectionPtr serverConn;

  server.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      serverConn = conn;
      ++serverConnected;
      if (serverConnected && clientConnected) {
        loop.queueInLoop([conn = serverConn]() { conn->forceClose(); });
      }
    } else {
      ++serverDisconnected;
    }
  });
  server.setStopCompleteCallback([&]() { loop.quit(); });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      if (serverConnected && clientConnected) {
        loop.queueInLoop([conn = serverConn]() { conn->forceClose(); });
      }
    } else {
      ++clientDisconnected;
      // loop.quit();
      loop.queueInLoop([&]() { server.stop(std::chrono::milliseconds(1000)); });
    }
  });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerHalfCloseCount;
    conn->shutdown();
  });

  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });

  loop.runAfter(std::chrono::milliseconds(2000), [&]() {
    std::cerr << "测试超时" << std::endl;
    std::abort();
  });

  server.start();
  client.connect();

  loop.loop();

  assert(serverConnected == 1);
  assert(serverDisconnected == 1);
  assert(clientConnected == 1);
  assert(clientDisconnected == 1);
  assert(clientPeerHalfCloseCount == 1);
}

void client_force_close_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);
  int reuse = 1;
  assert(0 == ::setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                           sizeof(reuse)));

  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  assert(0 == ::bind(serverFd,
                     reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                     serverAddressLen));

  assert(0 == ::getsockname(
                  serverFd,
                  reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                  &serverAddressLen));

  int serverConnected = 0;
  int serverDisconnected = 0;
  int clientConnected = 0;
  int clientDisconnected = 0;
  int serverPeerHalfCloseCount = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  TcpClient client(&loop, serverAddress);

  TcpConnectionPtr clientConn;

  server.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++serverConnected;
      if (serverConnected && clientConnected) {
        loop.queueInLoop([conn = clientConn]() { conn->forceClose(); });
      }
    } else {
      ++serverDisconnected;
      // loop.quit();
      loop.queueInLoop([&]() { server.stop(std::chrono::milliseconds(1000)); });
    }
  });
  server.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++serverPeerHalfCloseCount;
    conn->shutdown();
  });
  server.setStopCompleteCallback([&]() { loop.quit(); });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      clientConn = conn;
      if (serverConnected && clientConnected) {
        loop.queueInLoop([conn = clientConn]() { conn->forceClose(); });
      }
    } else {
      ++clientDisconnected;
    }
  });
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "conn error" << std::endl;
    std::abort();
  });

  server.start();
  client.connect();

  loop.runAfter(std::chrono::milliseconds(2000), [] {
    std::cerr << "client force close test timeout\n";
    std::abort();
  });

  loop.loop();
  assert(serverConnected == 1);
  assert(serverDisconnected == 1);
  assert(clientConnected == 1);
  assert(clientDisconnected == 1);
  assert(serverPeerHalfCloseCount == 1);
}

void cross_thread_force_close_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  int connected = 0;
  int disconnected = 0;
  std::promise<TcpConnectionPtr> connPromise;
  auto connFuture = connPromise.get_future();
  std::promise<void> closePromise;
  auto closeFuture = closePromise.get_future();

  EventLoopThread loopThread;

  EventLoop *loop = loopThread.startLoop();
  loop->queueInLoop([&] {
    auto conn = std::make_shared<TcpConnection>(loop, fds[0]);
    conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
      assert(loop->isInLoopThread());
      if (conn->connected()) {
        ++connected;
        connPromise.set_value(conn);
      } else {
        ++disconnected;
      }
    });

    conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
      assert(loop->isInLoopThread());
      conn->connectDestroyed();
      closePromise.set_value();
    });

    conn->connectEstablished();
  });

  auto conn = connFuture.get();
  conn->forceClose();

  const auto status = closeFuture.wait_for(std::chrono::milliseconds(2000));
  if (status != std::future_status::ready) {
    std::cerr << "cross thread force close test timeout\n";
    std::abort();
  }

  close(fds[1]);
  assert(connected == 1);
  assert(disconnected == 1);
}

void oversized_length_force_close_test() {
  int fds[2];
  assert(0 == socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                         fds));

  int connected = 0;
  int disconnected = 0;
  int closed = 0;
  int receivedMsg = 0;

  EventLoop loop;
  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);
  conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
    } else {
      ++disconnected;
    }
  });
  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++closed;
    conn->connectDestroyed();
  });
  conn->connectEstablished();

  LengthHeaderCodec codec;
  Buffer buffer;
  uint32_t msgLen = (64 << 20) + 1;
  uint32_t netMsglen = htonl(msgLen);
  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &msg) { ++receivedMsg; });

  buffer.append(reinterpret_cast<const char *>(&netMsglen), sizeof(uint32_t));

  codec.onMessage(conn, &buffer);

  char byte;
  assert(::read(fds[1], &byte, 1) == 0); // 对端 fds[0] 已关闭，读到 EOF
  close(fds[1]);

  assert(connected == 1);
  assert(disconnected == 1);
  assert(closed == 1);
  assert(receivedMsg == 0);
}

int main() {
  force_close_test();
  server_force_close_test();
  client_force_close_test();
  cross_thread_force_close_test();
  oversized_length_force_close_test();
}