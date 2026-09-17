#include "TcpConnection.h"
#include <assert.h>
#include <errno.h>
#include <iostream>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

TcpConnection::TcpConnection(EventLoop *loop, int fd)
    : TcpConnection(loop, fd, nullptr) {}

TcpConnection::TcpConnection(EventLoop *loop, int fd,
                             WriteFunction writeOperation)
    : loop_(loop), socket_(fd), state_(State::kConnecting),
      peerReadClosed_(false), writeShutdown_(false), shutdownRequested_(false),
      highWaterMark_(64 << 20), lowWaterMark_(highWaterMark_ >> 1),
      channel_(socket_.fd()), connectionCallback_(nullptr),
      messageCallback_(nullptr), peerHalfCloseCallback_(nullptr),
      closeCallback_(nullptr), writeCompleteCallback_(nullptr),
      highWaterMarkCallback_(nullptr), lowWaterMarkCallback_(nullptr),
      idleTimeout_(0), idleTimerId_(0),
      writeOperation_(std::move(writeOperation)) {

  assert(highWaterMark_ > 0);

  channel_.setReadCallback(std::bind(&TcpConnection::handleRead, this));
  channel_.setWriteCallback(std::bind(&TcpConnection::handleWrite, this));
  channel_.setCloseCallback(std::bind(&TcpConnection::handleHangup, this));
  channel_.setErrorCallback(std::bind(&TcpConnection::handleError, this));
}

TcpConnection::~TcpConnection() {}

void TcpConnection::setConnectionCallBack(ConnectionCallback cb) {
  assert(state_ == State::kConnecting);
  connectionCallback_ = cb;
}

void TcpConnection::setMessageCallback(MessageCallback cb) {
  assert(state_ == State::kConnecting);
  messageCallback_ = cb;
}

void TcpConnection::setPeerHalfCloseCallback(PeerHalfCloseCallback cb) {
  assert(state_ == State::kConnecting);
  peerHalfCloseCallback_ = cb;
}

void TcpConnection::setCloseCallback(CloseCallback cb) {
  assert(state_ == State::kConnecting);
  closeCallback_ = cb;
}

void TcpConnection::setWriteCompleteCallback(WriteCompleteCallback cb) {
  assert(state_ == State::kConnecting);
  writeCompleteCallback_ = cb;
}

void TcpConnection::setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                             size_t highWaterMark) {
  assert(state_ == State::kConnecting);
  assert(highWaterMark > 0);
  highWaterMarkCallback_ = std::move(cb);
  highWaterMark_ = highWaterMark;
}

void TcpConnection::setLowWaterMarkCallback(LowWaterMarkCallback cb,
                                            size_t lowWaterMark) {
  assert(state_ == State::kConnecting);
  lowWaterMarkCallback_ = std::move(cb);
  lowWaterMark_ = lowWaterMark;
}

void TcpConnection::connectEstablished() {
  assert(state_ == State::kConnecting);
  assert(lowWaterMark_ < highWaterMark_);

  state_ = State::kConnected;
  channel_.tie(shared_from_this());
  channel_.enableReading();
  loop_->updateChannel(&channel_);

  if (connectionCallback_) {
    connectionCallback_(shared_from_this());
  }

  if (state_ == State::kConnected) {
    refreshIdleTimer();
  }
}

void TcpConnection::connectDestroyed() {
  loop_->assertInLoopThread();

  channel_.disableAll();
  loop_->updateChannel(&channel_);

  socket_.close();
}

void TcpConnection::send(const char *data, size_t len) {
  if (loop_->isInLoopThread()) {
    sendInLoop(data, len);
  } else {
    std::string message(data, len);

    loop_->queueInLoop(
        [self = shared_from_this(), message = std::move(message)]() {
          self->sendInLoop(message.data(), message.size());
        });
  }
}

void TcpConnection::shutdown() {
  if (loop_->isInLoopThread()) {
    shutdownInLoop();
  } else {
    loop_->queueInLoop(
        [self = shared_from_this()]() { self->shutdownInLoop(); });
  }
}

void TcpConnection::setIdleTimeout(std::chrono::milliseconds timeout) {
  if (loop_->isInLoopThread()) {
    setIdleTimeoutInLoop(timeout);
  } else {
    loop_->queueInLoop([self = shared_from_this(), timeout] {
      self->setIdleTimeoutInLoop(timeout);
    });
  }
}

bool TcpConnection::connected() { return state_ == State::kConnected; }

void TcpConnection::forceClose() {
  if (loop_->isInLoopThread()) {
    forceCloseInLoop();
  } else {
    loop_->queueInLoop(
        [self = shared_from_this()]() { self->forceCloseInLoop(); });
  }
}

void TcpConnection::startRead() {
  if (loop_->isInLoopThread()) {
    startReadInLoop();
  } else {
    loop_->queueInLoop(
        [self = shared_from_this()]() { self->startReadInLoop(); });
  }
}

void TcpConnection::stopRead() {
  if (loop_->isInLoopThread()) {
    stopReadInLoop();
  } else {
    loop_->queueInLoop(
        [self = shared_from_this()]() { self->stopReadInLoop(); });
  }
}

void TcpConnection::handleRead() {
  if (state_ != State::kConnected) {
    return;
  }

  assert(state_ == State::kConnected);

  while (true) {
    char buf[1024] = {0};
    ssize_t readed = ::read(socket_.fd(), buf, sizeof(buf));

    if (readed > 0) {
      inputBuffer_.append(buf, readed);
      refreshIdleTimer();

      if (messageCallback_) {
        messageCallback_(shared_from_this(), &inputBuffer_);
      }

      if (state_ != State::kConnected || !channel_.isReadingEnabled()) {
        return;
      }

      continue;
    }

    if (readed == 0) {
      handlePeerHalfClose();
      return;
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }

    std::cout << "handle read error, " << strerror(errno) << std::endl;
    handleTerminalClose();
    return;
  }
}

void TcpConnection::handleWrite() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnected) {
    return;
  }

  while (outputBuffer_.readableBytes()) {
    ssize_t cnt;
    cnt = writeOperation(socket_.fd(), outputBuffer_.peek(),
                         outputBuffer_.readableBytes());

    if (cnt > 0) {
      size_t oldLen = outputBuffer_.readableBytes();
      assert(static_cast<size_t>(cnt) <= oldLen);
      outputBuffer_.retrieve(static_cast<size_t>(cnt));

      if (oldLen > lowWaterMark_ &&
          outputBuffer_.readableBytes() <= lowWaterMark_) {
        if (lowWaterMarkCallback_) {
          auto callback = lowWaterMarkCallback_;
          loop_->queueInLoop([callback = std::move(callback),
                              self = shared_from_this(),
                              bufferedBytes = outputBuffer_.readableBytes()]() {
            callback(self, bufferedBytes);
          });
        }
      }

      continue;
    }

    if (cnt == 0) {
      return;
    }

    if (errno == EINTR) {
      continue;
    }

    if (errno == EWOULDBLOCK || errno == EAGAIN) {
      return;
    }

    std::cout << "handle write error, " << strerror(errno) << std::endl;
    handleTerminalClose();
    return;
  }

  channel_.disableWriting();
  loop_->updateChannel(&channel_);

  if (writeCompleteCallback_) {
    writeCompleteCallback_(shared_from_this());
  }

  if (shutdownRequested_) {
    shutdownWriteInLoop();
  }
}

void TcpConnection::handlePeerHalfClose() {
  if (state_ != State::kConnected) {
    return;
  }

  assert(state_ == State::kConnected);

  if (peerReadClosed_) {
    return;
  }

  peerReadClosed_ = true;
  channel_.disableReading();
  loop_->updateChannel(&channel_);

  if (peerHalfCloseCallback_) {
    peerHalfCloseCallback_(shared_from_this());
  }

  if (shutdownRequested_) {
    shutdownWriteInLoop();
  }
}

void TcpConnection::handleError() {
  if (state_ != State::kConnected) {
    return;
  }

  handleTerminalClose();
}

void TcpConnection::handleHangup() {
  if (state_ != State::kConnected) {
    return;
  }

  handleTerminalClose();
}

void TcpConnection::shutdownWriteInLoop() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnected || !shutdownRequested_ ||
      channel_.isWritingEnabled()) {
    return;
  }

  if (!writeShutdown_) {
    if (socket_.shutdownWrite() < 0) {
      std::cerr << "shutdown(SHUT_WR) error: " << strerror(errno) << std::endl;
      return;
    }

    writeShutdown_ = true; // 成功后才标记
  }

  if (peerReadClosed_) {
    handleTerminalClose();
  }
}

void TcpConnection::handleTerminalClose() {
  if (state_ == State::kDisconnected) {
    return;
  }

  if (idleTimerId_ != 0) {
    loop_->cancel(idleTimerId_);
    idleTimerId_ = 0;
  }

  state_ = State::kDisconnected;
  channel_.disableAll();
  loop_->updateChannel(&channel_);

  if (connectionCallback_) {
    connectionCallback_(shared_from_this());
  }

  if (closeCallback_) {
    closeCallback_(shared_from_this());
  }
}

void TcpConnection::sendInLoop(const char *data, size_t len) {
  loop_->assertInLoopThread();

  if (state_ != State::kConnected || shutdownRequested_ || !len) {
    return;
  }

  ssize_t written = 0;
  size_t remaining = len;
  if (!channel_.isWritingEnabled() && !outputBuffer_.readableBytes()) {
    written = writeOperation(socket_.fd(), data, len);
  }

  if (written > 0) {
    assert(static_cast<size_t>(written) <= len);
    remaining = len - static_cast<size_t>(written);
  } else if (written < 0) {
    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
      written = 0;
    } else {
      std::cerr << "send writing error: " << strerror(errno) << std::endl;
      handleTerminalClose();
      return;
    }
  }

  if (remaining > 0) {
    size_t oldLen = outputBuffer_.readableBytes();
    outputBuffer_.append(data + static_cast<size_t>(written), remaining);

    if (!channel_.isWritingEnabled()) {
      channel_.enableWriting();
      loop_->updateChannel(&channel_);
    }

    if (oldLen < highWaterMark_ && oldLen + remaining >= highWaterMark_) {
      if (highWaterMarkCallback_) {
        auto callback = highWaterMarkCallback_;

        loop_->queueInLoop([self = shared_from_this(),
                            callback = std::move(callback),
                            bufferLen = outputBuffer_.readableBytes()]() {
          callback(self, bufferLen);
        });
      }
    }
  }
}

void TcpConnection::shutdownInLoop() {
  loop_->assertInLoopThread();

  if (state_ != State::kConnected || shutdownRequested_) {
    return;
  }

  shutdownRequested_ = true;
  shutdownWriteInLoop();
}

void TcpConnection::setIdleTimeoutInLoop(std::chrono::milliseconds timeout) {
  loop_->assertInLoopThread();

  if (idleTimerId_ != 0) {
    loop_->cancel(idleTimerId_);
    idleTimerId_ = 0;
  }

  idleTimeout_ = timeout;

  if (state_ == State::kConnected && idleTimeout_.count() > 0) {
    refreshIdleTimer();
  }
}

void TcpConnection::refreshIdleTimer() {
  loop_->assertInLoopThread();
  if (idleTimeout_.count() <= 0) {
    return;
  }

  if (idleTimerId_ != 0) {
    loop_->cancel(idleTimerId_);
  }

  std::weak_ptr<TcpConnection> weak = shared_from_this();
  idleTimerId_ = loop_->runAfter(idleTimeout_, [weak] {
    if (auto conn = weak.lock()) {
      conn->handleIdleTimeout();
    }
  });
}

void TcpConnection::handleIdleTimeout() {
  loop_->assertInLoopThread();

  idleTimerId_ = 0;
  handleTerminalClose();
}

void TcpConnection::forceCloseInLoop() {
  loop_->assertInLoopThread();

  if (state_ == State::kConnected) {
    handleTerminalClose();
  }
}

ssize_t TcpConnection::writeOperation(int fd, const void *buf, size_t n) {
  ssize_t ret = 0;
  if (writeOperation_) {
    ret = writeOperation_(fd, buf, n);
  } else {
    ret = ::write(fd, buf, n);
  }
  return ret;
}

void TcpConnection::startReadInLoop() {
  loop_->assertInLoopThread();
  if (state_ != State::kConnected || peerReadClosed_) {
    return;
  }

  if (!channel_.isReadingEnabled()) {
    channel_.enableReading();
    loop_->updateChannel(&channel_);
  }
}

void TcpConnection::stopReadInLoop() {
  loop_->assertInLoopThread();
  if (state_ != State::kConnected) {
    return;
  }

  if (channel_.isReadingEnabled()) {
    channel_.disableReading();
    loop_->updateChannel(&channel_);
  }
}
