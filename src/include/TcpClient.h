#ifndef MINI_MUDUO_TCP_CLIENT_H
#define MINI_MUDUO_TCP_CLIENT_H

#include "InetAddress.h"
#include "TcpConnection.h"
#include "TimerId.h"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>

class EventLoop;
class Connector;

using ConnectionErrorCallback = std::function<void(int)>;

class TcpClient {
public:
  explicit TcpClient(EventLoop *loop, const InetAddress &serverAddr);
  explicit TcpClient(EventLoop *loop, const InetAddress &serverAddr,
                     WriteFunction writeOperation);

  ~TcpClient();

  /**
   * 开始建立连接
   */
  void connect();

  /**
   * 优雅关闭
   */
  void disconnect();

  /**
   * 停止未完成的连接建立
   */
  void stop();

  /**
   * 连接成功后获取TcpConnection
   */
  TcpConnectionPtr connection() const;

  /**
   * TcpConnection的回调函数
   */
  void setConnectionCallback(ConnectionCallback cb);

  void setMessageCallback(MessageCallback cb);

  void setPeerHalfCloseCallback(PeerHalfCloseCallback cb);

  void setWriteCompleteCallback(WriteCompleteCallback cb);

  void setConnectionErrorCallback(ConnectionErrorCallback cb);

  void enableRetry();

  void disableRetry();

  void setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                size_t highWaterMark = 64 * 1024 * 1024);
  void setLowWaterMarkCallback(LowWaterMarkCallback cb,
                               size_t lowWaterMark = 32 * 1024 * 1024);

private:
  enum class State { kDisconnected, kConnecting, kConnected, kDisconnecting };

private:
  void newConnection(int sockfd);

  void connectionError(int error);

  void removeConnection(const TcpConnectionPtr &conn);

  void scheduleRetry();

  void cancelRetry();

  void retry();

  void assertCanConfigure();

private:
  EventLoop *loop_;
  InetAddress serverAddr_;
  State state_;
  std::shared_ptr<Connector> connector_;
  TcpConnectionPtr tcpConnection_;

  bool retryEnabled_;
  bool wantConnect_;
  TimerId retryTimerId_;
  const std::chrono::milliseconds retryInterval_;

  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  PeerHalfCloseCallback peerHalfCloseCallback_;
  WriteCompleteCallback writeCompleteCallback_;
  ConnectionErrorCallback connectionErrorCallback_;

  HighWaterMarkCallback highWaterMarkCallback_;
  LowWaterMarkCallback lowWaterMarkCallback_;
  size_t highWaterMark_;
  size_t lowWaterMark_;

  WriteFunction writeOperation_;
};

#endif