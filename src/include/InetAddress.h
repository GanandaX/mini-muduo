#ifndef MINI_MUDUO_INET_ADDRESS_H
#define MINI_MUDUO_INET_ADDRESS_H

#include <cstdint>
#include <netinet/in.h>
#include <string>

class InetAddress {

public:
  explicit InetAddress(uint16_t port);
  explicit InetAddress(const sockaddr_in &addr);
  explicit InetAddress(const char *ip, uint16_t port);
  ~InetAddress();

  const sockaddr_in &getSockAddr() const;
  sockaddr_in *getSockAddr();

  std::string toIp() const;
  std::string toIpPort() const;
  uint16_t port() const;

private:
  sockaddr_in addr_;
};

#endif