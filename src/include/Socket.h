#ifndef MINI_MUDUO_SOCKET_H
#define MINI_MUDUO_SOCKET_H
#include "InetAddress.h"

class Socket {
public:
  explicit Socket(int fd);
  ~Socket();
  
  Socket(const Socket &) = delete;
  Socket &operator=(const Socket &) = delete;

  int fd() const;

  void bindAddress(const InetAddress &localAddress);

  void listen();

  int accept(InetAddress *peerAddress);

  int shutdownWrite();

  void setReuseAddress(bool on);

  void close();

private:
  int fd_;
};

#endif