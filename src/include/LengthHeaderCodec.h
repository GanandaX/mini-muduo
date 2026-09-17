#ifndef MINI_MUDUO_LENGTH_HEADER_CODE_H
#define MINI_MUDUO_LENGTH_HEADER_CODE_H

#include "TcpConnection.h"

#include <cstdint>
#include <functional>
#include <string>

class LengthHeaderCodec {
public:
  using StringMessageCallback =
      std::function<void(const TcpConnectionPtr &, const std::string &)>;

  void send(const TcpConnectionPtr &conn, const char *msg, size_t msgLen);
  void onMessage(const TcpConnectionPtr &conn, Buffer *buffer);
  void setMessageCallback(StringMessageCallback cb);

private:
  static constexpr size_t kMaxMessageSize = 64 << 20;
  static constexpr size_t kHeaderLen = sizeof(uint32_t);

  StringMessageCallback messageCallback_;
};

#endif