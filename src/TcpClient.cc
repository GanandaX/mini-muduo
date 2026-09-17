#include "TcpClient.h"
#include "Connector.h"
#include "EventLoop.h"

#include <assert.h>
#include <iostream>
#include <unistd.h>

TcpClient::TcpClient(EventLoop *loop, const InetAddress &serverAddr)
    : TcpClient(loop, serverAddr, nullptr) {}

TcpClient::TcpClient(EventLoop *loop, const InetAddress &serverAddr,
                     WriteFunction writeFunction)
    : loop_(loop), serverAddr_(serverAddr), state_(State::kDisconnected),
      retryEnabled_(false), wantConnect_(false), retryTimerId_(0),
      retryInterval_(std::chrono::milliseconds(100)),
      highWaterMarkCallback_(nullptr), lowWaterMarkCallback_(nullptr),
      highWaterMark_(64 * 1024 * 1024), lowWaterMark_(32 * 1024 * 1024),
      writeOperation_(std::move(writeFunction)) {
  assert(loop != nullptr);
  connector_ = std::make_shared<Connector>(loop, serverAddr_);
  connector_->setNewConnectionCallback(
      std::bind(&TcpClient::newConnection, this, std::placeholders::_1));
  connector_->setConnectionErrorCallback(
      std::bind(&TcpClient::connectionError, this, std::placeholders::_1));
}

TcpClient::~TcpClient() {
  loop_->assertInLoopThread();
  cancelRetry();
  assert(state_ == State::kDisconnected);
  assert(!tcpConnection_);
}

void TcpClient::connect() {
  loop_->assertInLoopThread();

  wantConnect_ = true;
  cancelRetry();
  if (state_ != State::kDisconnected) {
    return;
  }

  assert(!tcpConnection_);
  state_ = State::kConnecting;
  connector_->restart();
}

void TcpClient::disconnect() {
  loop_->assertInLoopThread();

  wantConnect_ = false;
  cancelRetry();

  if (state_ == State::kConnecting) {
    stop();
    return;
  }

  if (state_ == State::kConnected) {
    assert(tcpConnection_);
    state_ = State::kDisconnecting;
    tcpConnection_->shutdown();
    return;
  }

  return;
}

void TcpClient::stop() {
  loop_->assertInLoopThread();

  wantConnect_ = false;
  cancelRetry();
  if (state_ != State::kConnecting) {
    return;
  }

  state_ = State::kDisconnected;
  connector_->stop();
}

TcpConnectionPtr TcpClient::connection() const { return tcpConnection_; }

void TcpClient::setConnectionCallback(ConnectionCallback cb) {
  assertCanConfigure();
  connectionCallback_ = std::move(cb);
}

void TcpClient::setMessageCallback(MessageCallback cb) {
  assertCanConfigure();
  messageCallback_ = std::move(cb);
}

void TcpClient::setPeerHalfCloseCallback(PeerHalfCloseCallback cb) {
  assertCanConfigure();
  peerHalfCloseCallback_ = std::move(cb);
}

void TcpClient::setWriteCompleteCallback(WriteCompleteCallback cb) {
  assertCanConfigure();
  writeCompleteCallback_ = std::move(cb);
}

void TcpClient::setConnectionErrorCallback(ConnectionErrorCallback cb) {
  assertCanConfigure();
  connectionErrorCallback_ = std::move(cb);
}

void TcpClient::enableRetry() {
  loop_->assertInLoopThread();
  retryEnabled_ = true;
  if (wantConnect_ && state_ == State::kDisconnected) {
    scheduleRetry();
  }
}

void TcpClient::disableRetry() {
  loop_->assertInLoopThread();
  retryEnabled_ = false;
  cancelRetry();
}

void TcpClient::setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                         size_t highWaterMark) {
  assertCanConfigure();
  assert(highWaterMark > 0);
  highWaterMarkCallback_ = std::move(cb);
  highWaterMark_ = highWaterMark;
}

void TcpClient::setLowWaterMarkCallback(LowWaterMarkCallback cb,
                                        size_t lowWaterMark) {
  assertCanConfigure();
  lowWaterMarkCallback_ = std::move(cb);
  lowWaterMark_ = lowWaterMark;
}

void TcpClient::newConnection(int sockfd) {
  loop_->assertInLoopThread();

  if (state_ != State::kConnecting) {
    std::cerr << __func__ << " state error" << std::endl;
    ::close(sockfd);
    return;
  }

  tcpConnection_ =
      std::make_shared<TcpConnection>(loop_, sockfd, writeOperation_);

  tcpConnection_->setConnectionCallBack(connectionCallback_);
  tcpConnection_->setMessageCallback(messageCallback_);
  tcpConnection_->setPeerHalfCloseCallback(peerHalfCloseCallback_);
  tcpConnection_->setWriteCompleteCallback(writeCompleteCallback_);
  tcpConnection_->setCloseCallback(
      std::bind(&TcpClient::removeConnection, this, std::placeholders::_1));

  tcpConnection_->setLowWaterMarkCallback(lowWaterMarkCallback_, lowWaterMark_);
  tcpConnection_->setHighWaterMarkCallback(highWaterMarkCallback_,
                                           highWaterMark_);

  state_ = State::kConnected;

  tcpConnection_->connectEstablished();
}

void TcpClient::connectionError(int error) {
  loop_->assertInLoopThread();

  if (state_ != State::kConnecting) {
    return;
  }

  state_ = State::kDisconnected;

  if (connectionErrorCallback_) {
    connectionErrorCallback_(error);
  }

  scheduleRetry();
}

void TcpClient::removeConnection(const TcpConnectionPtr &conn) {
  loop_->assertInLoopThread();
  if (tcpConnection_ != conn) {
    std::cerr << __func__ << " conn changed" << std::endl;
    return;
  }

  auto oldConn = conn;
  tcpConnection_.reset();
  state_ = State::kDisconnected;
  oldConn->connectDestroyed();

  scheduleRetry();
}

void TcpClient::scheduleRetry() {
  loop_->assertInLoopThread();

  if (retryTimerId_ != 0) {
    return;
  }

  if (!(retryEnabled_ && wantConnect_) || (state_ != State::kDisconnected)) {
    return;
  }

  retryTimerId_ = loop_->runAfter(retryInterval_, [this]() { retry(); });
}

void TcpClient::cancelRetry() {
  loop_->assertInLoopThread();
  if (retryTimerId_) {
    TimerId oldTimerId = retryTimerId_;
    retryTimerId_ = 0;
    loop_->cancel(oldTimerId);
  }
}

void TcpClient::retry() {
  loop_->assertInLoopThread();

  retryTimerId_ = 0;
  if (!(retryEnabled_ && wantConnect_) || (state_ != State::kDisconnected)) {
    return;
  }

  state_ = State::kConnecting;
  connector_->restart();
}

void TcpClient::assertCanConfigure() {
  loop_->assertInLoopThread();
  assert(state_ == State::kDisconnected);
  assert(!wantConnect_);
  assert(!tcpConnection_);
}