#ifndef MINI_MUDUO_TCP_CONNECTION_H
#define MINI_MUDUO_TCP_CONNECTION_H

#include "Buffer.h"
#include "Channel.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"
#include "TimerId.h"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <sys/types.h>

class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using ConnectionCallback = std::function<void(const TcpConnectionPtr &)>;
using MessageCallback = std::function<void(const TcpConnectionPtr &, Buffer *)>;
using PeerHalfCloseCallback = std::function<void(const TcpConnectionPtr &)>;
using CloseCallback = std::function<void(const TcpConnectionPtr &)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr &)>;
using HighWaterMarkCallback =
    std::function<void(const TcpConnectionPtr &, size_t bufferedBytes)>;
using LowWaterMarkCallback =
    std::function<void(const TcpConnectionPtr &, size_t bufferedBytes)>;
using WriteFunction = std::function<ssize_t(int, const void *, size_t)>;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {

public:
  explicit TcpConnection(EventLoop *loop, int fd);
  explicit TcpConnection(EventLoop *loop, int fd, WriteFunction writeOperation);

  ~TcpConnection();

  int fd() const { return socket_.fd(); }

  void setConnectionCallBack(ConnectionCallback cb);
  void setMessageCallback(MessageCallback cb);
  void setPeerHalfCloseCallback(PeerHalfCloseCallback cb);
  void setCloseCallback(CloseCallback cb);
  void setWriteCompleteCallback(WriteCompleteCallback cb);
  void setHighWaterMarkCallback(HighWaterMarkCallback cb,
                                size_t highWaterMark = 64 * 1024 * 1024);
  void setLowWaterMarkCallback(LowWaterMarkCallback cb,
                               size_t lowWaterMark = 32 * 1024 * 1024);

  void connectEstablished();

  /**
   * 连接对象最终销毁前的收尾：
   * 确保 Channel 已从 epoll 注销，并关闭 fd。
   * 必须在该连接所属的 EventLoop 线程执行。
   */
  void connectDestroyed();

  void send(const char *data, size_t len);
  void shutdown();
  EventLoop *loop() const { return loop_; }

  void setIdleTimeout(std::chrono::milliseconds timeout);

  bool connected();

  void forceClose();

  void startRead();

  void stopRead();

private:
  void handleRead();
  void handleWrite();

  /**
   * 确认对端已关闭写方向（read() 返回 0）：
   * 停止监听读事件；若本端已请求关闭写，则尝试完成 shutdown(SHUT_WR)。
   * 不直接销毁 TcpConnection。
   */
  void handlePeerHalfClose();
  void handleError();
  void handleHangup();

  void shutdownWriteInLoop();

  /**
   * 连接进入终态：
   * 注销 Channel 的全部事件，并通知上层回收该 TcpConnection。
   * 不在此处 close(fd_)；fd 由 connectDestroyed() 统一关闭。
   */
  void handleTerminalClose();

  void sendInLoop(const char *data, size_t len);
  void shutdownInLoop();

  void setIdleTimeoutInLoop(std::chrono::milliseconds timeout);

  void refreshIdleTimer();
  void handleIdleTimeout();

  void forceCloseInLoop();

  ssize_t writeOperation(int fd, const void *buf, size_t n);

  void startReadInLoop();

  void stopReadInLoop();

private:
  enum class State { kConnecting, kConnected, kDisconnected };

  EventLoop *loop_;
  Socket socket_;
  State state_;
  bool peerReadClosed_;
  bool writeShutdown_;
  bool shutdownRequested_;

  size_t highWaterMark_;
  size_t lowWaterMark_;

  Channel channel_;
  Buffer inputBuffer_;
  Buffer outputBuffer_;

  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  PeerHalfCloseCallback peerHalfCloseCallback_;
  CloseCallback closeCallback_;
  WriteCompleteCallback writeCompleteCallback_;
  HighWaterMarkCallback highWaterMarkCallback_;
  LowWaterMarkCallback lowWaterMarkCallback_;

  std::chrono::milliseconds idleTimeout_;
  TimerId idleTimerId_;

  WriteFunction writeOperation_;
};

#endif