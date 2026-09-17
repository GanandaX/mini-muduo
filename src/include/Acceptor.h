#ifndef MINI_MUDUO_ACCEPTOR_H
#define MINI_MUDUO_ACCEPTOR_H
#include "Channel.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"

#include <functional>
#include <netinet/in.h>
#include <sys/socket.h>

class Acceptor {

public:
  using NewConnectionCallback =
      std::function<void(int clientfd, const InetAddress &)>;

  // 若返回负数，调用方约定它必须像 accept4() 一样正确设置 errno
  using AcceptFunction =
      std::function<int(int fd, sockaddr *addr, socklen_t *addrLen, int flags)>;

  explicit Acceptor(EventLoop *loop, int listenFd);
  explicit Acceptor(EventLoop *loop, int listenFd,
                    AcceptFunction acceptFunction);
  virtual ~Acceptor();

  void setNewConnectionCallback(NewConnectionCallback cb);

  void listen();
  void stopAccepting();
  void startAccepting();
  void close();

private:
  void handleRead();

  int handleAccept(InetAddress &peerAddr);

private:
  enum class State {
    kNotListening,
    kAccepting,
    kPaused,
    kClosed,
  };

  int idleFd_;
  State state_;
  EventLoop *loop_;
  Socket acceptSocket_;
  Channel acceptChannel_;

  NewConnectionCallback newConnectionCallback_;
  AcceptFunction acceptFunction_;
};

#endif
