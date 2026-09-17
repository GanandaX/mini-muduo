#ifndef MINI_MUDUO_CONNECTOR_H
#define MINI_MUDUO_CONNECTOR_H

#include "InetAddress.h"

#include <functional>
#include <memory>

class Channel;
class EventLoop;

class Connector : public std::enable_shared_from_this<Connector> {
public:
  using NewConnectionCallback = std::function<void(int)>;
  using ConnectionErrorCallback = std::function<void(int)>;

  explicit Connector(EventLoop *loop, const InetAddress &serverAddr);

  ~Connector();

  void setNewConnectionCallback(NewConnectionCallback cb);
  void setConnectionErrorCallback(ConnectionErrorCallback cb);

  void start();
  void stop();
  void restart();

private:
  enum class State {
    kDisconnected,
    kConnecting,
    kConnected,
  };

private:
  void startInLoop();
  void stopInLoop();
  void restartInLoop();
  void connecting(int sockfd);

  void handleWrite();
  void handleError();

  int removeAndResetChannel();

  void resetChannel();

private:
  EventLoop *loop_;
  InetAddress serverAddr_;
  State state_;

  std::unique_ptr<Channel> channel_;
  NewConnectionCallback newConnectionCallback_;
  ConnectionErrorCallback errorCallback_;
};

#endif