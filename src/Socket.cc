#include "Socket.h"
#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string.h>
#include <unistd.h>

Socket::Socket(int fd) : fd_(fd) { assert(fd_ >= 0); }

Socket::~Socket() { close(); }

int Socket::fd() const { return fd_; }

void Socket::bindAddress(const InetAddress &localAddress) {
  assert(fd_ >= 0);

  int ret = ::bind(
      fd_, reinterpret_cast<const sockaddr *>(&localAddress.getSockAddr()),
      sizeof(sockaddr_in));

  if (ret < 0) {
    std::cerr << "bind error: " << strerror(errno) << '\n';
    std::abort();
  }
}

void Socket::listen() {
  assert(fd_ >= 0);

  int ret = ::listen(fd_, SOMAXCONN);

  if (ret < 0) {
    std::cerr << "listen error: " << strerror(errno) << '\n';
    std::abort();
  }
}

int Socket::accept(InetAddress *peerAddress) {
  assert(fd_ >= 0);

  socklen_t socklen = sizeof(sockaddr_in);

  return ::accept4(fd_,
                   reinterpret_cast<sockaddr *>(peerAddress->getSockAddr()),
                   &socklen, SOCK_NONBLOCK | SOCK_CLOEXEC);
}

int Socket::shutdownWrite() {
  assert(fd_ >= 0);

  return ::shutdown(fd_, SHUT_WR);
}

void Socket::setReuseAddress(bool on) {
  assert(fd_ >= 0);

  int enable = on ? 1 : 0;

  int ret =
      ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

  if (ret < 0) {
    std::cerr << "setReuseAddress error, " << strerror(errno) << std::endl;
    std::abort();
  }
}

void Socket::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}
