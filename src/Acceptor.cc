#include "Acceptor.h"
#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <ostream>
#include <sys/socket.h>
#include <unistd.h>

Acceptor::Acceptor(EventLoop *loop, int listenFd)
    : Acceptor(loop, listenFd, AcceptFunction{}) {}

Acceptor::Acceptor(EventLoop *loop, int listenFd, AcceptFunction acceptFunction)
    : idleFd_(::open("/dev/null", O_RDONLY | O_CLOEXEC)),
      state_(State::kNotListening), loop_(loop), acceptSocket_(listenFd),
      acceptChannel_(acceptSocket_.fd()),
      acceptFunction_(std::move(acceptFunction)) {
  assert(loop_ != nullptr);
  acceptChannel_.setReadCallback(std::bind(&Acceptor::handleRead, this));
  if (idleFd_ < 0) {
    std::cerr << "idle fd open error" << std::endl;
    std::abort();
  }
}

Acceptor::~Acceptor() {
  loop_->assertInLoopThread();
  close();
}

void Acceptor::setNewConnectionCallback(NewConnectionCallback cb) {
  newConnectionCallback_ = cb;
}

void Acceptor::listen() {
  loop_->assertInLoopThread();
  if (state_ != State::kNotListening) {
    return;
  }

  acceptSocket_.listen();

  acceptChannel_.enableReading();
  loop_->updateChannel(&acceptChannel_);
  state_ = State::kAccepting;
}

void Acceptor::stopAccepting() {
  loop_->assertInLoopThread();
  if (state_ != State::kAccepting) {
    return;
  }

  state_ = State::kPaused;
  acceptChannel_.disableReading();
  loop_->updateChannel(&acceptChannel_);
}

void Acceptor::startAccepting() {
  loop_->assertInLoopThread();
  if (state_ != State::kPaused) {
    return;
  }

  state_ = State::kAccepting;
  acceptChannel_.enableReading();
  loop_->updateChannel(&acceptChannel_);
}

void Acceptor::close() {
  loop_->assertInLoopThread();

  if (state_ == State::kClosed) {
    return;
  }

  state_ = State::kClosed;
  acceptChannel_.disableAll();
  loop_->updateChannel(&acceptChannel_);
  acceptSocket_.close();

  if (idleFd_ >= 0) {
    ::close(idleFd_);
    idleFd_ = -1;
  }
}

void Acceptor::handleRead() {
  while (true && (state_ == State::kAccepting)) {
    InetAddress peerAddr(0);
    int clientFd = handleAccept(peerAddr);

    if (clientFd >= 0) {
      if (newConnectionCallback_) {
        newConnectionCallback_(clientFd, peerAddr);
      } else {
        ::close(clientFd);
      }
      continue;
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == EMFILE) {
      if (idleFd_ != -1) {
        ::close(idleFd_);
        idleFd_ = -1;

        clientFd = handleAccept(peerAddr);

        if (clientFd >= 0) {
          ::close(clientFd);
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
          std::cerr << "client fd accept4 error: " << strerror(errno)
                    << std::endl;
        }

        idleFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (idleFd_ < 0) {
          std::cerr << "idle fd reopen failed" << std::endl;
          std::abort();
        }
      } else {
        std::cerr << "idle fd == -1 && errno == EMFILE " << std::endl;
      }
      break;
    }

    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      break;
    }

    std::cerr << "accept4 error: " << strerror(errno) << std::endl;
    break;
  }
}

int Acceptor::handleAccept(InetAddress &peerAddr) {
  int clientFd;
  if (acceptFunction_) {
    socklen_t socklen = sizeof(sockaddr_in);
    clientFd =
        acceptFunction_(acceptSocket_.fd(),
                        reinterpret_cast<sockaddr *>(peerAddr.getSockAddr()),
                        &socklen, SOCK_NONBLOCK | SOCK_CLOEXEC);
  } else {
    clientFd = acceptSocket_.accept(&peerAddr);
  }

  return clientFd;
}
