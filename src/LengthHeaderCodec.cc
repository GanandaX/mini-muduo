#include "LengthHeaderCodec.h"

#include <arpa/inet.h>
#include <cstddef>
#include <cstring>
#include <iostream>

void LengthHeaderCodec::send(const TcpConnectionPtr &conn, const char *msg,
                             size_t msgLen) {
  if (msgLen > kMaxMessageSize) {
    std::cerr << "message len out of capacity, " << msgLen << std::endl;
    return;
  }

  const size_t packetLen = kHeaderLen + msgLen;
  uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

  std::string packet;
  packet.resize(packetLen);

  ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
  ::memcpy(packet.data() + kHeaderLen, msg, msgLen);

  conn->send(packet.data(), packetLen);
}

void LengthHeaderCodec::onMessage(const TcpConnectionPtr &conn,
                                  Buffer *buffer) {
  while (buffer->readableBytes() >= kHeaderLen) {
    uint32_t networkLen;
    ::memcpy(&networkLen, buffer->peek(), kHeaderLen);
    const size_t msgLen = ntohl(networkLen);

    if (msgLen > kMaxMessageSize) {
      conn->forceClose();
      return;
    }

    if (msgLen > (buffer->readableBytes() - kHeaderLen)) {
      return;
    }

    std::string msg(buffer->peek() + kHeaderLen, msgLen);
    buffer->retrieve(msgLen + kHeaderLen);

    if (messageCallback_) {
      messageCallback_(conn, msg);
    }

    if(conn && !conn->connected()) {
      return;
    }
  }
}

void LengthHeaderCodec::setMessageCallback(StringMessageCallback cb) {
  messageCallback_ = std::move(cb);
}