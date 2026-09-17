#ifndef MINI_MUDUO_TCP_SERVER_H
#define MINI_MUDUO_TCP_SERVER_H

#include "Acceptor.h"
#include "EventLoop.h"
#include "EventLoopThreadPool.h"
#include "InetAddress.h"
#include "TcpConnection.h"
#include "TimerId.h"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <unordered_map>

class TcpServer {

public:
  using StopCompleteCallback = std::function<void()>;

  explicit TcpServer(EventLoop *loop, int listenFd, int threadNum = 0,
                     WriteFunction writeOperation = nullptr);

  ~TcpServer();

  void setConnectionCallback(ConnectionCallback cb);
  void setMessageCallback(MessageCallback cb);
  void setPeerHalfCloseCallback(PeerHalfCloseCallback cb);
  void setWriteCompleteCallback(WriteCompleteCallback cb);
  void setStopCompleteCallback(StopCompleteCallback cb);

  void setIdleTimeout(std::chrono::milliseconds idleTimeout);

  void start();
  void stopAccepting();
  void startAccepting();
  void stop(std::chrono::milliseconds gracePeriod);

  void setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                size_t highWaterMark = 64 * 1024 * 1024);
  void setLowWaterMarkCallback(LowWaterMarkCallback cb,
                               size_t lowWaterMark = 32 * 1024 * 1024);

private:
  void newConnection(int clientFd, const InetAddress &peerAddr);
  void removeConnection(const TcpConnectionPtr &conn);
  void removeConnectionInLoop(const TcpConnectionPtr &conn);

  void startInLoop();
  void stopAcceptingInLoop();
  void startAcceptingInLoop();
  void stopInLoop(std::chrono::milliseconds gracePeriod);

  void shutdownAllConnectionsInLoop();
  void forceCloseAllConnectionsInLoop();
  void finishStopIfNeededInLoop();
  void handleStopDeadline();

  void cancelStopTimer();
 
  void assertCanConfigure() const;

private:
  enum class Status { kNotStarted, kRunning, kStopping, kStopped };

  Status status_;
  TimerId stopDeadlineTimerId_;

  std::chrono::milliseconds idleTimeout_;
  EventLoop *loop_;
  std::unique_ptr<EventLoopThreadPool> threadPool_;
  Acceptor acceptor_;

  std::unordered_map<int, TcpConnectionPtr> connections_;

  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  PeerHalfCloseCallback peerHalfCloseCallback_;
  WriteCompleteCallback writeCompleteCallback_;
  // 服务端已停止接收，且 connections_ 已清空；不保证各连接的 connectDestroyed()
  // 或对端 FIN 通知已完成
  StopCompleteCallback stopCompleteCallback_;

  HighWaterMarkCallback highWaterMarkCallback_;
  LowWaterMarkCallback lowWaterMarkCallback_;
  size_t highWaterMark_;
  size_t lowWaterMark_;

  WriteFunction writeOperation_;
};

#endif