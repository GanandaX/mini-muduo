#include "Acceptor.h"
#include <arpa/inet.h>
#include <errno.h>
#include <error.h>
#include <iostream>
#include <string.h>
#include <unistd.h>

int createListenSocket(uint16_t port) {

  do {
    int sock_fd =
        socket(PF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (sock_fd < 0) {
      // std::cout << "create listen socket error, err_msg: " << strerror(errno)
      //           << std::endl;
      // return errno;
      break;
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int reuse = 1;
    int setsockopt_res =
        setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (setsockopt_res) {
      int errno_copy = errno;
      close(sock_fd);
      errno = errno_copy;
      break;
    }

    int bind_res =
        bind(sock_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(sockaddr_in));

    if (!bind_res) {
      return sock_fd;
    }

    int errno_copy = errno;
    close(sock_fd);
    errno = errno_copy;
  } while (false);

  std::cout << "create listen socket error, err_msg: " << strerror(errno)
            << std::endl;

  exit(-1);
}

int main() {
  EventLoop loop;

  int listenFd = createListenSocket(2007);

  Acceptor acceptor(&loop, listenFd);
  acceptor.setNewConnectionCallback(
      [&](int clientFd, const InetAddress &clientAddr) {
        char buf[128] = {0};
        inet_ntop(AF_INET, &clientAddr.getSockAddr().sin_addr.s_addr, buf, sizeof(buf));
        std::cout << "accepted: " << buf << " : " << ntohs(clientAddr.getSockAddr().sin_port)
                  << std::endl;
        close(clientFd);
      });

  acceptor.listen();
  loop.loop();
}