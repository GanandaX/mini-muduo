#include <arpa/inet.h>
#include <assert.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <csignal>
#include "Buffer.h"
#include "TcpServer.h"

enum class ConnectionStatus { kKeepAlive, kPeerClosed, kError, kWaitClose };

typedef struct tcp_connect {
  int fd;
  int flags;
  Buffer outputBuffer;
  bool waiting_close;
} tcp_connect_t;

int createListenSocket(uint16_t port);
void setNonBlocking(int fd);
void createEpoll();
void closeFd(int fd);
int appendToEpoll(int append_fd, int events, tcp_connect_t *connection);
void deleteConnect(int fd);
tcp_connect_t *createConnect(int fd);
int removeFromEpoll(int remove_fd);
int updateEpoll(int fd);
ConnectionStatus readMsg(int clientfd);
ConnectionStatus writeMsg(int clientfd);

int epfd = 0;
std::map<int, tcp_connect_t *> connections;

int createListenSocket(uint16_t port) {

  do {
    int sock_fd = socket(PF_INET, SOCK_STREAM, 0);
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

void setNonBlocking(int fd) {

  assert(fd >= 0);
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0) {
    std::cout << "fcntl F_GETFL error" << std::endl;
    exit(-1);
  }

  int set_res = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  if (set_res < 0) {
    std::cout << "fcntl FSETFL error" << std::endl;
    exit(-1);
  }
}

void createEpoll() {

  epfd = epoll_create1(EPOLL_CLOEXEC);
  if (epfd < 0) {
    std::cout << "epoll create error" << std::endl;
    exit(-1);
  }
}

void closeFd(int fd) {
  if (close(fd) < 0) {
    std::cout << "close error, msg: " << strerror(errno) << std::endl;
  }
}

int appendToEpoll(int append_fd, int events, tcp_connect_t *connection) {

  epoll_event event;
  memset(&event, 0, sizeof(epoll_event));
  event.events = events;
  event.data.ptr = connection;
  connection->flags = events;

  int epoll_ctl_res = epoll_ctl(epfd, EPOLL_CTL_ADD, append_fd, &event);
  if (epoll_ctl_res) {
    int errno_copy = errno;
    closeFd(append_fd);
    errno = errno_copy;

    deleteConnect(append_fd);
    std::cout << "epoll add error, msg = " << strerror(errno) << std::endl;
    return -1;
  }

  return 0;
}

void deleteConnect(int fd) {
  if (connections.find(fd) == connections.end()) {
    std::cout << "deleteConnect not found connection" << std::endl;
    exit(-1);
  }

  tcp_connect_t *connection = connections[fd];
  connections.erase(fd);

  if (connection) {
    delete connection;
  }
}

tcp_connect_t *createConnect(int fd) {

  if (connections.find(fd) != connections.end()) {
    return nullptr;
  }

  tcp_connect_t *connection = new tcp_connect;
  if (!connection) {
    std::cout << "alloc connection error" << std::endl;
    return nullptr;
  }

  connection->fd = fd;
  connection->waiting_close = false;
  connections[fd] = connection;
  return connection;
}

int removeFromEpoll(int remove_fd) {

  int epoll_ctl_res = epoll_ctl(epfd, EPOLL_CTL_DEL, remove_fd, nullptr);
  if (epoll_ctl_res) {
    std::cout << "epoll add error, msg = " << strerror(errno) << std::endl;
  }

  closeFd(remove_fd);
  deleteConnect(remove_fd);
  return 0;
}

int updateEpoll(int fd) {

  if (connections.find(fd) == connections.end()) {
    std::cout << "update poll not found connection" << std::endl;
    exit(-1);
  }

  tcp_connect_t *connection = connections[fd];

  epoll_event event;
  memset(&event, 0, sizeof(epoll_event));
  event.events = connection->flags;
  event.data.ptr = connection;

  int epoll_ctl_res = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event);
  if (epoll_ctl_res) {
    int errno_copy = errno;
    removeFromEpoll(fd);
    errno = errno_copy;
    std::cout << "epoll update error, msg = " << strerror(errno) << std::endl;
    return -1;
  }

  return 0;
}

ConnectionStatus readMsg(int clientfd) {
  char buf[1024] = {0};

  if (connections.find(clientfd) == connections.end()) {
    std::cout << "error, not found connection struct !" << std::endl;
    exit(-1);
  }

  tcp_connect_t *connection = connections[clientfd];

  while (true) {
    memset(buf, 0, sizeof(buf));
    ssize_t ret = read(clientfd, buf, sizeof(buf));

    if (ret > 0) {
      std::cout.write(buf, ret);
      connection->outputBuffer.append(buf, ret);

      connection->flags |= EPOLLOUT;
      updateEpoll(connection->fd);

      continue;
    }

    if (ret == 0) {
      return ConnectionStatus::kPeerClosed;
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return ConnectionStatus::kKeepAlive;
    }

    std::cout << "read error, error msg: " << strerror(errno) << std::endl;
    return ConnectionStatus::kError;
  }
}

ConnectionStatus writeMsg(int clientfd) {
  int ret = 0;

  if (connections.find(clientfd) == connections.end()) {
    std::cout << "error, not found connection struct !" << std::endl;
    exit(-1);
  }

  tcp_connect_t *connection = connections[clientfd];

  while (true) {

    if (!connection->outputBuffer.readableBytes()) {
      connection->flags &= ~EPOLLOUT;
      updateEpoll(connection->fd);
      return ConnectionStatus::kKeepAlive;
    }

    ret = write(clientfd, connection->outputBuffer.peek(),
                connection->outputBuffer.readableBytes());

    if (ret > 0) {
      connection->outputBuffer.retrieve(ret);
      continue;
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return ConnectionStatus::kKeepAlive;
    }

    std::cout << "read error, error msg: " << strerror(errno) << std::endl;
    return ConnectionStatus::kError;
  }
}

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::cout << "mini-muduo epoll echo server\n";

  uint16_t server_port = 2007;
  int server_fd = createListenSocket(server_port);
  setNonBlocking(server_fd);

  EventLoop loop;
  TcpServer tcpServer(&loop, server_fd);

  tcpServer.setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    conn->send(buffer->peek(), buffer->readableBytes());
    buffer->retrieve(buffer->readableBytes());
  });
  tcpServer.setPeerHalfCloseCallback([&](const TcpConnectionPtr& conn) {
    conn->shutdown();
  });

  tcpServer.start();

  return 0;
}
/* 
int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::cout << "mini-muduo epoll echo server\n";

  uint16_t server_port = 2007;
  int server_fd = createListenSocket(server_port);
  setNonBlocking(server_fd);

  int listen_res = listen(server_fd, 64);
  if (listen_res < 0) {
    std::cout << "listen error" << std::endl;
    exit(-1);
  }

  createEpoll();

  tcp_connect_t *server_connection = createConnect(server_fd);
  appendToEpoll(server_fd, EPOLLIN, server_connection);

  epoll_event epoll_events[128];

  while (1) {

    int events_cnt = epoll_wait(epfd, epoll_events, 128, -1);

    if (events_cnt < 0) {
      if (errno == EINTR) {
        continue;
      }

      std::cout << "epoll wait error, msg: " << strerror(errno) << std::endl;
      break;
    }

    sockaddr_in client_addr;
    char ip_buf[128];
    for (int i = 0; i < events_cnt; i++) {

      tcp_connect_t *connection =
          reinterpret_cast<tcp_connect_t *>(epoll_events[i].data.ptr);

      if ((connection->fd == server_fd) && (epoll_events[i].events & EPOLLIN)) {

        while (true) {
          memset(&client_addr, 0, sizeof(sockaddr_in));
          socklen_t addr_len = sizeof(client_addr);
          int client_fd =
              accept4(server_fd, reinterpret_cast<sockaddr *>(&client_addr),
                      &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);

          if (client_fd >= 0) {
            inet_ntop(AF_INET, &client_addr.sin_addr.s_addr, ip_buf,
                      sizeof(ip_buf));
            std::cout << "client : " << ip_buf << ":"
                      << ntohs(client_addr.sin_port) << std::endl;

            tcp_connect_t *client_connection = createConnect(client_fd);
            if (!client_connection) {
              closeFd(client_fd);
              break;
            }

            if (appendToEpoll(client_fd, EPOLLIN | EPOLLRDHUP,
                              client_connection)) {
              break;
            }
          } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
              break;
            } else if (errno == EINTR) {
              continue;
            }

            std::cout << "accept error, msg: " << strerror(errno) << std::endl;
            break;
          }
        }

        continue;
      }

      if (epoll_events[i].events & (EPOLLIN | EPOLLRDHUP)) {
        ConnectionStatus read_res = readMsg(connection->fd);
        if (read_res != ConnectionStatus::kKeepAlive) {
          connection->waiting_close = true;
        }
      }

      if (epoll_events[i].events & EPOLLOUT) {
        ConnectionStatus write_res = writeMsg(connection->fd);
        if (write_res != ConnectionStatus::kKeepAlive) {
          connection->waiting_close = true;
        }
      }

      if (epoll_events[i].events & (EPOLLERR | EPOLLHUP)) {
        connection->waiting_close = true;
      }

      if (connection->waiting_close && !connection->outputBuffer.readableBytes()) {
        removeFromEpoll(connection->fd);
      }
    }
  }

  return 0;
} */