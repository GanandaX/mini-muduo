#include "Channel.h"
#include "EventLoop.h"
#include "Socket.h"
#include "TcpClient.h"
#include "TcpServer.h"

#include <assert.h>
#include <iostream>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

void connect_success_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  socket.setReuseAddress(true);

  InetAddress serverAddr("127.0.0.1", 0);
  socket.bindAddress(serverAddr);
  socklen_t serverAddrLen = sizeof(*serverAddr.getSockAddr());
  assert(::getsockname(socket.fd(),
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverAddrLen) == 0);
  std::cout << "server address:" << serverAddr.toIpPort() << std::endl;
  socket.listen();

  bool connected = false;
  bool serverReceived = false;
  bool loopQuited = false;
  bool unexpectedError = false;

  EventLoop loop;
  Channel serverChannel(socket.fd());
  serverChannel.setReadCallback([&]() {
    InetAddress clientAddr(0);
    int clientfd = socket.accept(&clientAddr);

    std::cout << "client address:" << clientAddr.toIpPort() << std::endl;
    serverReceived = true;
    assert(clientfd >= 0);
    ::close(clientfd);
  });
  serverChannel.enableReading();
  loop.updateChannel(&serverChannel);

  TcpClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      connected = true;
      client.disconnect();
    } else {
      loopQuited = true;
      loop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int error) {
    loopQuited = true;
    unexpectedError = true;
    loop.quit();
  });

  client.connect();

  loop.loop();
  loop.removeChannel(&serverChannel);

  assert(connected);
  assert(serverReceived);
  assert(loopQuited);
  assert(!unexpectedError);
}

void connect_error_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  socket.bindAddress(serverAddress);

  assert(
      ::getsockname(socket.fd(),
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddressLen) == 0);

  std::cout << "server address:" << serverAddress.toIpPort() << std::endl;

  int error_code = 0;
  EventLoop loop;

  TcpClient client(&loop, serverAddress);
  client.setConnectionCallback(
      [&](const TcpConnectionPtr &) { assert(false); });

  client.setConnectionErrorCallback([&](int error) {
    error_code = error;
    loop.quit();
  });

  client.connect();
  loop.loop();

  assert(error_code == ECONNREFUSED);
}

void stop_test() {
  Socket socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  socket.bindAddress(serverAddress);

  assert(
      ::getsockname(socket.fd(),
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddressLen) == 0);

  std::cout << "server address:" << serverAddress.toIpPort() << std::endl;

  bool connected = false;
  bool connectError = false;
  EventLoop loop;

  TcpClient client(&loop, serverAddress);
  client.setConnectionCallback(
      [&](const TcpConnectionPtr &) { connected = true; });

  client.setConnectionErrorCallback([&](int) { connectError = true; });

  client.connect();
  client.stop();
  loop.runAfter(std::chrono::milliseconds(50), [&]() { loop.quit(); });

  loop.loop();

  assert(!connected);
  assert(!connectError);
}

void echo_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);
  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  assert(::bind(serverFd,
                reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                serverAddressLen) == 0);

  assert(
      ::getsockname(serverFd,
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddressLen) == 0);

  std::cout << "server address:" << serverAddress.toIpPort() << std::endl;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  server.setMessageCallback([&](const TcpConnectionPtr conn, Buffer *buffer) {
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
    conn->shutdown();
  });
  server.start();

  bool sendedMsg = false;
  bool recvedMsg = false;
  bool disconnected = false;
  bool unexpectedError = false;

  char buf[1024] = "hello world!";
  uint16_t len = strlen(buf);

  TcpClient client(&loop, serverAddress);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      conn->send(buf, len);
      sendedMsg = true;
    } else {
      disconnected = true;
      loop.quit();
    }
  });

  client.setMessageCallback([&](const TcpConnectionPtr &, Buffer *buffer) {
    assert(std::string(buffer->peek(), buffer->readableBytes()) == buf);
    std::cout << "recv(" << buffer->readableBytes() << ") : " << buffer->peek()
              << std::endl;
    recvedMsg = true;
    client.disconnect();
  });

  client.setConnectionErrorCallback([&](int) {
    unexpectedError = true;
    loop.quit();
  });

  client.connect();

  loop.loop();
  assert(sendedMsg);
  assert(recvedMsg);
  assert(disconnected);
  assert(!unexpectedError);
}

void restart_echo_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);
  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(*serverAddress.getSockAddr());
  assert(::bind(serverFd,
                reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                serverAddressLen) == 0);

  assert(
      ::getsockname(serverFd,
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddressLen) == 0);

  std::cout << "server address:" << serverAddress.toIpPort() << std::endl;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  server.setMessageCallback([&](const TcpConnectionPtr conn, Buffer *buffer) {
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
    conn->shutdown();
  });
  server.start();

  bool unexpectedError = false;

  int connectedCount = 0;
  int receivedCount = 0;
  int disconnectedCount = 0;

  char buf[1024] = "hello world!";
  const size_t len = strlen(buf);

  TcpClient client(&loop, serverAddress);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connectedCount;
      conn->send(buf, len);
    } else {
      ++disconnectedCount;

      if (disconnectedCount == 1) {
        loop.queueInLoop([&]() { client.connect(); });
      } else if (disconnectedCount == 2) {
        loop.quit();
      } else {
        assert(false);
      }
    }
  });

  client.setMessageCallback([&](const TcpConnectionPtr &, Buffer *buffer) {
    assert(std::string(buffer->peek(), buffer->readableBytes()) == buf);
    std::cout << "recv(" << buffer->readableBytes()
              << ") : " << std::string(buffer->peek(), buffer->readableBytes())
              << std::endl;
    ++receivedCount;
    client.disconnect();
  });

  client.setConnectionErrorCallback([&](int) {
    unexpectedError = true;
    loop.quit();
  });

  client.connect();

  loop.loop();

  assert(connectedCount == 2);
  assert(receivedCount == 2);
  assert(disconnectedCount == 2);

  assert(!unexpectedError);
}

void auto_retry_echo_test() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddress("127.0.0.1", 0);
  socklen_t serverAddressLen = sizeof(sockaddr_in);
  assert(::bind(serverFd,
                reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                serverAddressLen) == 0);

  assert(
      ::getsockname(serverFd,
                    reinterpret_cast<sockaddr *>(serverAddress.getSockAddr()),
                    &serverAddressLen) == 0);

  int serverRecvCnt = 0;
  int clientSndCnt = 0;
  int clientRecvCnt = 0;
  int connectSucessCnt = 0;
  int connectErrorCnt = 0;
  bool serverStarted = false;

  EventLoop loop;
  TcpServer server(&loop, serverFd);

  server.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++serverRecvCnt;
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
    conn->shutdown();
  });
  server.setStopCompleteCallback([&]() {
      loop.quit();
  });

  char msg[] = "hello world!";
  size_t msgLen = strlen(msg);

  TcpClient client(&loop, serverAddress);
  client.enableRetry();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connectSucessCnt;
      ++clientSndCnt;
      conn->send(msg, msgLen);
    } else {
      loop.queueInLoop([&]() {
        server.stop(std::chrono::milliseconds(1000));
      });
    }
  });
  client.setConnectionErrorCallback([&](int) {
    ++connectErrorCnt;
    if (!serverStarted) {
      serverStarted = true;
      server.start();
    }
  });
  client.setMessageCallback([&](const TcpConnectionPtr &, Buffer *buffer) {
    ++clientRecvCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) == msg);
    std::cout << "recv(" << buffer->readableBytes()
              << ") : " << std::string(buffer->peek(), buffer->readableBytes())
              << std::endl;
    client.disconnect();
  });

  client.connect();

  loop.runAfter(std::chrono::milliseconds(2000), []() {
    std::cerr << "auto retry test timeout\n";
    std::abort();
  });

  loop.loop();

  assert(serverRecvCnt == 1);
  assert(clientSndCnt == 1);
  assert(clientRecvCnt == 1);
  assert(connectSucessCnt == 1);
  assert(connectErrorCnt >= 1);
  assert(serverStarted);
}

int main() {
  //    connect_success_test();
  //    connect_error_test();
  //    stop_test();
  //    echo_test();
  //    restart_echo_test();
  auto_retry_echo_test();
}