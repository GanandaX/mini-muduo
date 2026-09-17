#include "Connector.h"
#include "EventLoop.h"
#include "Socket.h"
#include "TcpServer.h"

#include <arpa/inet.h>
#include <assert.h>
#include <iostream>
#include <memory>
#include <sys/socket.h>
#include <unistd.h>

void success_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  InetAddress addr("127.0.0.1", 0);

  socket.bindAddress(addr);

  socklen_t len = sizeof(sockaddr_in);
  ::getsockname(socket.fd(), reinterpret_cast<sockaddr *>(addr.getSockAddr()),
                &len);
  socket.listen();

  bool connected = false;
  int connectError = 0;

  EventLoop loop;
  auto connector = std::make_shared<Connector>(&loop, addr);
  connector->setNewConnectionCallback([&](int clientfd) {
    connected = true;
    std::cout << "client fd: " << clientfd << std::endl;
    ::close(clientfd);
    loop.quit();
  });
  connector->setConnectionErrorCallback([&](int error) {
    connectError = error;
    loop.quit();
  });

  connector->start();
  loop.loop();

  assert(connected);
  assert(connectError == 0);
}

void error_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));

  InetAddress addr("127.0.0.1", 0);
  socket.bindAddress(addr);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  assert(::getsockname(socket.fd(),
                       reinterpret_cast<sockaddr *>(addr.getSockAddr()),
                       &serverAddrLen) == 0);

  char buf[16] = {};
  std::cout << "server : " << addr.toIpPort() << std::endl;

  bool connected = false;
  int connectError = 0;

  EventLoop loop;
  auto connector = std::make_shared<Connector>(&loop, addr);
  connector->setNewConnectionCallback([&](int clientfd) {
    std::cout << "client fd: " << clientfd << std::endl;
    connected = true;
    ::close(clientfd);
    loop.quit();
  });

  connector->setConnectionErrorCallback([&](int error) {
    connectError = error;
    loop.quit();
  });

  connector->start();
  loop.loop();

  assert(connected == false);
  assert(connectError == ECONNREFUSED);
}

void stop_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));

  assert(socket.fd() >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverAddrLen = sizeof(sockaddr_in);
  socket.bindAddress(serverAddr);
  assert(::getsockname(socket.fd(),
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen) == 0);
  std::cout << "server:  " << serverAddr.toIpPort() << std::endl;

  socket.listen();

  bool connected = false;
  bool gotError = false;

  EventLoop loop;
  auto connector = std::make_shared<Connector>(&loop, serverAddr);
  connector->setConnectionErrorCallback([&](int error) { gotError = true; });
  connector->setNewConnectionCallback([&](int clientfd) {
    connected = clientfd;
    ::close(clientfd);
  });

  connector->start();
  connector->stop();
  loop.runAfter(std::chrono::milliseconds(50), [&]() { loop.quit(); });
  loop.loop();

  assert(connected == false);
  assert(!gotError);
}

int main() { stop_test(); }