#include "TcpServer.h"

#include <assert.h>
#include <string.h>
#include <sys/socket.h>

int main() {
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(fd >= 0);

  int reuse = 1;
  assert(::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) ==
         0);

  sockaddr_in addr{};
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(2007);
  socklen_t len = sizeof(addr);

  ::bind(fd, (sockaddr *)(&addr), len);

  EventLoop baseLoop;
  TcpServer server(&baseLoop, fd, 2);

  server.setMessageCallback([](const TcpConnectionPtr &conn, Buffer *buffer) {
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
  });
  server.setPeerHalfCloseCallback([](const TcpConnectionPtr &conn) {
    conn->shutdown();
  });

  server.start();
  baseLoop.loop();
  return 0;
}