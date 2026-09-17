#include "Acceptor.h"
#include "EventLoopThread.h"
#include "TcpClient.h"
#include "TcpConnection.h"

#include <assert.h>
#include <future>
#include <iostream>
#include <unistd.h>

void emfile_recovery_drops_one_connection_and_accepts_next_test() {
  std::atomic<int> acceptorFunctionCnt = 0;
  std::atomic<int> newConnectionCnt = 0;
  int connectCnt = 0;
  int disconnectCnt = 0;
  int peerCloseCnt = 0;

  std::promise<void> clientStartConnectPromose;
  auto clientStartConnectFuture = clientStartConnectPromose.get_future();
  std::promise<void> serverDestroyedPromise;
  auto serverDestroyedFuture = serverDestroyedPromise.get_future();

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddress.getSockAddr()),
                serverAddrLen));

  assert(
      ::getsockname(serverFd,
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddrLen) == 0);

  EventLoopThread eventLoopThread;
  auto threadEventLoop = eventLoopThread.startLoop();
  std::unique_ptr<Acceptor> acceptor = std::make_unique<Acceptor>(
      threadEventLoop, serverFd,
      [&](int fd, sockaddr *addr, socklen_t *addrLen, int flags) -> int {
        if (++acceptorFunctionCnt == 1) {
          errno = EMFILE;
          return -1;
        }

        return ::accept4(fd, addr, addrLen, flags);
      });

  acceptor->setNewConnectionCallback(
      [&](int clientfd, const InetAddress &addr) {
        ++newConnectionCnt;
        ::close(clientfd);
      });

  EventLoop loop;
  TcpClient client(&loop, serverAddress);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connectCnt;
    } else {
      ++disconnectCnt;

      if (disconnectCnt == 1) {
        assert(newConnectionCnt == 0);
        loop.queueInLoop([&] { client.connect(); });
      } else if (disconnectCnt == 2) {
        loop.quit();
      }
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerCloseCnt;
    conn->shutdown();
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });

  loop.runAfter(std::chrono::seconds(2), [&] {
    std::cerr << " run timeout " << std::endl;
    std::abort();
  });

  threadEventLoop->queueInLoop([&]() {
    acceptor->listen();
    clientStartConnectPromose.set_value();
  });
  auto waitForRes =
      clientStartConnectFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "acceptor listen timeout" << std::endl;
    std::abort();
  }
  client.connect();
  loop.loop();

  threadEventLoop->queueInLoop([&] {
    acceptor.reset();
    serverDestroyedPromise.set_value();
  });
  waitForRes = serverDestroyedFuture.wait_for(std::chrono::milliseconds(100));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "acceptor listen timeout" << std::endl;
    std::abort();
  }

  assert(acceptorFunctionCnt == 4);
  assert(newConnectionCnt == 1);
  assert(connectCnt == 2);
  assert(disconnectCnt == 2);
  assert(peerCloseCnt == 2);
}

int main() { emfile_recovery_drops_one_connection_and_accepts_next_test(); }