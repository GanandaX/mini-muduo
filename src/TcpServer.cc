#include "TcpServer.h"
#include <assert.h>
#include <unistd.h>

TcpServer::TcpServer(EventLoop *loop, int listenFd, int threadNum,
                     WriteFunction writeOperation)
    : status_(Status::kNotStarted), stopDeadlineTimerId_(0), idleTimeout_(0),
      loop_(loop),
      threadPool_(std::make_unique<EventLoopThreadPool>(loop, threadNum)),
      acceptor_(loop, listenFd), highWaterMarkCallback_(nullptr),
      lowWaterMarkCallback_(nullptr), highWaterMark_(64 * 1024 * 1024),
      lowWaterMark_(32 * 1024 * 1024),
      writeOperation_(std::move(writeOperation)) {
  acceptor_.setNewConnectionCallback(std::bind(&TcpServer::newConnection, this,
                                               std::placeholders::_1,
                                               std::placeholders::_2));
}

TcpServer::~TcpServer() {
  loop_->assertInLoopThread();
  assert(status_ == Status::kStopped || status_ == Status::kNotStarted);
  assert(connections_.empty());
  assert(stopDeadlineTimerId_ == 0);
}

void TcpServer::setConnectionCallback(ConnectionCallback cb) {
  assertCanConfigure();
  connectionCallback_ = std::move(cb);
}

void TcpServer::setMessageCallback(MessageCallback cb) {
  assertCanConfigure();
  messageCallback_ = std::move(cb);
}

void TcpServer::setPeerHalfCloseCallback(PeerHalfCloseCallback cb) {
  assertCanConfigure();
  peerHalfCloseCallback_ = std::move(cb);
}

void TcpServer::setWriteCompleteCallback(WriteCompleteCallback cb) {
  assertCanConfigure();
  writeCompleteCallback_ = std::move(cb);
}

void TcpServer::setStopCompleteCallback(StopCompleteCallback cb) {
  assertCanConfigure();
  stopCompleteCallback_ = std::move(cb);
}

void TcpServer::setIdleTimeout(std::chrono::milliseconds idleTimeout) {
  assertCanConfigure();
  assert(idleTimeout.count() >= 0);
  idleTimeout_ = idleTimeout;
}

void TcpServer::start() {
  if (loop_->isInLoopThread()) {
    startInLoop();
  } else {
    loop_->queueInLoop([&]() { startInLoop(); });
  }
}

void TcpServer::stopAccepting() {
  if (loop_->isInLoopThread()) {
    stopAcceptingInLoop();
  } else {
    loop_->queueInLoop([&]() { stopAcceptingInLoop(); });
  }
}

void TcpServer::startAccepting() {
  if (loop_->isInLoopThread()) {
    startAcceptingInLoop();
  } else {
    loop_->queueInLoop([&]() { startAcceptingInLoop(); });
  }
}

void TcpServer::stop(std::chrono::milliseconds gracePeriod) {
  if (loop_->isInLoopThread()) {
    stopInLoop(gracePeriod);
  } else {
    loop_->queueInLoop(
        [&, gracePeriod = gracePeriod]() { stopInLoop(gracePeriod); });
  }
}

void TcpServer::setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                         size_t highWaterMark) {
  assertCanConfigure();
  assert(highWaterMark > 0);
  highWaterMarkCallback_ = std::move(cb);
  highWaterMark_ = highWaterMark;
}

void TcpServer::setLowWaterMarkCallback(LowWaterMarkCallback cb,
                                        size_t lowWaterMark) {
  assertCanConfigure();
  lowWaterMarkCallback_ = std::move(cb);
  lowWaterMark_ = lowWaterMark;
}

void TcpServer::newConnection(int clientFd, const InetAddress &peerAddr) {
  loop_->assertInLoopThread();

  if (status_ != Status::kRunning) {
    ::close(clientFd);
    return;
  }

  EventLoop *ioLoop = threadPool_->getNextLoop();

  auto newConnection =
      std::make_shared<TcpConnection>(ioLoop, clientFd, writeOperation_);
  connections_[clientFd] = newConnection;

  newConnection->setConnectionCallBack(connectionCallback_);
  newConnection->setMessageCallback(messageCallback_);
  newConnection->setPeerHalfCloseCallback(peerHalfCloseCallback_);
  newConnection->setWriteCompleteCallback(writeCompleteCallback_);
  newConnection->setCloseCallback(
      std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));

  newConnection->setIdleTimeout(idleTimeout_);

  newConnection->setLowWaterMarkCallback(lowWaterMarkCallback_, lowWaterMark_);
  newConnection->setHighWaterMarkCallback(highWaterMarkCallback_,
                                          highWaterMark_);

  ioLoop->queueInLoop(
      [newConnection]() { newConnection->connectEstablished(); });
}

void TcpServer::removeConnection(const TcpConnectionPtr &conn) {
  loop_->queueInLoop([this, conn] { removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr &conn) {
  loop_->assertInLoopThread();

  connections_.erase(conn->fd());
  conn->loop()->queueInLoop([conn] { conn->connectDestroyed(); });
  finishStopIfNeededInLoop();
}

void TcpServer::startInLoop() {
  loop_->assertInLoopThread();

  if (status_ != Status::kNotStarted) {
    return;
  }

  status_ = Status::kRunning;
  threadPool_->start();
  acceptor_.listen();
}

void TcpServer::stopAcceptingInLoop() {
  loop_->assertInLoopThread();

  if (status_ != Status::kRunning) {
    return;
  }
  acceptor_.stopAccepting();
}

void TcpServer::startAcceptingInLoop() {
  loop_->assertInLoopThread();

  if (status_ != Status::kRunning) {
    return;
  }
  acceptor_.startAccepting();
}

void TcpServer::stopInLoop(std::chrono::milliseconds gracePeriod) {
  assert(gracePeriod.count() >= 0);
  loop_->assertInLoopThread();

  if (stopDeadlineTimerId_ != 0 || status_ != Status::kRunning) {
    return;
  }

  status_ = Status::kStopping;
  acceptor_.close();

  if (connections_.empty()) {
    finishStopIfNeededInLoop();
    return;
  }

  if (gracePeriod > std::chrono::milliseconds(0)) {
    stopDeadlineTimerId_ =
        loop_->runAfter(gracePeriod, [&]() { handleStopDeadline(); });
    shutdownAllConnectionsInLoop();
    return;
  }

  forceCloseAllConnectionsInLoop();
}

void TcpServer::shutdownAllConnectionsInLoop() {
  loop_->assertInLoopThread();

  if (status_ != Status::kStopping) {
    return;
  }

  std::unordered_map<int, TcpConnectionPtr> connections(connections_.begin(),
                                                        connections_.end());
  for (auto &conn : connections) {
    conn.second->shutdown();
  }
}

void TcpServer::forceCloseAllConnectionsInLoop() {
  loop_->assertInLoopThread();

  if (status_ != Status::kStopping) {
    return;
  }

  cancelStopTimer();
  std::unordered_map<int, TcpConnectionPtr> connections(connections_.begin(),
                                                        connections_.end());
  for (auto &conn : connections) {
    conn.second->forceClose();
  }
}

void TcpServer::finishStopIfNeededInLoop() {
  loop_->assertInLoopThread();

  if (status_ == Status::kStopping && connections_.empty()) {
    cancelStopTimer();
    status_ = Status::kStopped;
    if (stopCompleteCallback_) {
      stopCompleteCallback_();
    }
    return;
  }
}

void TcpServer::handleStopDeadline() {
  loop_->assertInLoopThread();

  if (status_ != Status::kStopping) {
    return;
  }

  cancelStopTimer();
  forceCloseAllConnectionsInLoop();
}

void TcpServer::cancelStopTimer() {
  if (stopDeadlineTimerId_ != 0) {
    loop_->cancel(stopDeadlineTimerId_);
    stopDeadlineTimerId_ = 0;
  }
}

void TcpServer::assertCanConfigure() const {
  assert(status_ == Status::kNotStarted);
}