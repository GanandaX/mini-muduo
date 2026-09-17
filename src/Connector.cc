#include "Connector.h"
#include "Channel.h"
#include "EventLoop.h"

#include <assert.h>
#include <cerrno>
#include <iostream>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
int getSocketError(int fd) {
  assert(fd >= 0);
  int error = 0;
  socklen_t len = sizeof(error);

  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
    return errno;
  }
  return error;
}
} // namespace

Connector::Connector(EventLoop *loop, const InetAddress &serverAddr)
    : loop_(loop), serverAddr_(serverAddr), state_(State::kDisconnected) {
  assert(loop != nullptr);
}

Connector::~Connector() {}

void Connector::setNewConnectionCallback(NewConnectionCallback cb) {
  newConnectionCallback_ = cb;
}

void Connector::setConnectionErrorCallback(ConnectionErrorCallback cb) {
  errorCallback_ = cb;
}

void Connector::start() {
  if (loop_->isInLoopThread()) {
    startInLoop();
  } else {
    loop_->queueInLoop([self = shared_from_this()] { self->startInLoop(); });
  }
}

void Connector::stop() {
  if (loop_->isInLoopThread()) {
    stopInLoop();
  } else {
    loop_->queueInLoop([self = shared_from_this()] { self->stopInLoop(); });
  }
}

void Connector::restart() {
  if (loop_->isInLoopThread()) {
    restartInLoop();
  } else {
    loop_->queueInLoop(
        [self = shared_from_this()]() { self->restartInLoop(); });
  }
}

void Connector::startInLoop() {
  loop_->assertInLoopThread();

  if (state_ != State::kDisconnected) {
    std::cerr << __func__ << " state error" << std::endl;
    return;
  }

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

  if (fd < 0) {
    const int savedErrno = errno;
    state_ = State::kDisconnected;
    ::close(fd);
    std::cerr << "Connector startup error, " << strerror(errno) << std::endl;
    if (errorCallback_) {
      errorCallback_(savedErrno);
    }
    return;
  }

  int ret = ::connect(
      fd, reinterpret_cast<const sockaddr *>(serverAddr_.getSockAddr()),
      sizeof(sockaddr_in));
  int savedErrno = errno;

  if (ret == 0) {
    state_ = State::kConnected;
    if (newConnectionCallback_) {
      newConnectionCallback_(fd);
    } else {
      ::close(fd);
      state_ = State::kDisconnected;
    }
  } else if (savedErrno == EINPROGRESS) {
    connecting(fd);
  } else {
    state_ = State::kDisconnected;
    ::close(fd);

    if (errorCallback_) {
      errorCallback_(savedErrno);
    }
  }
}

void Connector::stopInLoop() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnecting) {
    std::cerr << __func__ << " state error" << std::endl;
    return;
  }

  state_ = State::kDisconnected;
  int fd = removeAndResetChannel();
  ::close(fd);
}

void Connector::restartInLoop() {
  loop_->assertInLoopThread();

  if (state_ == State::kConnecting) {
    return;
  }

  if (state_ == State::kConnected) {
    state_ = State::kDisconnected;
  }

  startInLoop();
}

void Connector::connecting(int sockfd) {
  state_ = State::kConnecting;
  channel_ = std::make_unique<Channel>(sockfd);
  channel_->setWriteCallback(std::bind(&Connector::handleWrite, this));
  channel_->setErrorCallback(std::bind(&Connector::handleError, this));
  channel_->tie(shared_from_this());
  channel_->enableWriting();
  loop_->updateChannel(channel_.get());
}

void Connector::handleWrite() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnecting) {
    return;
  }

  int error = getSocketError(channel_->fd());

  if (error != 0) {
    handleError();
    return;
  }

  int sockfd = removeAndResetChannel();
  state_ = State::kConnected;

  if (newConnectionCallback_) {
    newConnectionCallback_(sockfd);
  } else {
    ::close(sockfd);
    state_ = State::kDisconnected;
  }
}

void Connector::handleError() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnecting) {
    std::cerr << __func__ << " state error" << std::endl;
    return;
  }
  int error = getSocketError(channel_->fd());

  int sockfd = removeAndResetChannel();
  state_ = State::kDisconnected;
  ::close(sockfd);

  if (errorCallback_) {
    errorCallback_(error);
  }
}

int Connector::removeAndResetChannel() {
  loop_->assertInLoopThread();
  assert(channel_ != nullptr);

  int sockfd = channel_->fd();

  channel_->disableAll();
  loop_->removeChannel(channel_.get());
  loop_->queueInLoop([self = shared_from_this()]() { self->resetChannel(); });

  return sockfd;
}

void Connector::resetChannel() {
  loop_->assertInLoopThread();
  channel_.reset();
}