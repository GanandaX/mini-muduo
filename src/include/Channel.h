#ifndef MINI_MUDUO_CHANNEL_H
#define MINI_MUDUO_CHANNEL_H

#include <cstdint>
#include <functional>
#include <memory>
#include <sys/epoll.h>

class Channel {

public:
  using EventCallback = std::function<void()>;

  explicit Channel(int fd);

  int fd() const;
  uint32_t events() const;
  uint32_t revents() const;

  void setRevents(uint32_t revents);

  bool isReadingEnabled() const;
  bool isWritingEnabled() const;

  void enableReading();
  void enableWriting();
  void disableReading();
  void disableWriting();
  void disableAll();

  void setReadCallback(EventCallback cb);
  void setWriteCallback(EventCallback cb);
  void setCloseCallback(EventCallback cb);
  void setErrorCallback(EventCallback cb);

  void handleEvent();

  void tie(const std::shared_ptr<void> &owner);

private:
  void handleEventWithGuard();

private:
  static constexpr uint32_t kReadEvents = EPOLLIN | EPOLLPRI | EPOLLRDHUP;

  int fd_;
  uint32_t events_;
  uint32_t revents_;

  EventCallback readCallback_;
  EventCallback writeCallback_;
  EventCallback closeCallback_;
  EventCallback errorCallback_;

  bool tied_{false};
  std::weak_ptr<void> tie_;
};

#endif