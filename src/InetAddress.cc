#include "InetAddress.h"

#include <arpa/inet.h>
#include <iostream>
#include <string.h>

InetAddress::InetAddress(uint16_t port) : addr_{} {
  addr_.sin_family = AF_INET;
  addr_.sin_addr.s_addr = htonl(INADDR_ANY);
  addr_.sin_port = htons(port);
}

InetAddress::InetAddress(const sockaddr_in &addr) : addr_(addr) {}

InetAddress::InetAddress(const char *ip, uint16_t port) : addr_{} {
  int ret = ::inet_pton(AF_INET, ip, &addr_.sin_addr.s_addr);
  if (ret != 1) {
    std::cerr << "InetAddress conver failed" << std::endl;
    abort();
  }

  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);
}

InetAddress::~InetAddress() {}

const sockaddr_in &InetAddress::getSockAddr() const { return addr_; }

sockaddr_in *InetAddress::getSockAddr() { return &addr_; }

std::string InetAddress::toIp() const {
  char buffer[INET_ADDRSTRLEN] = {};
  ::inet_ntop(AF_INET, &addr_.sin_addr.s_addr, buffer, sizeof(buffer));
  return buffer;
}

std::string InetAddress::toIpPort() const {
  return toIp() + ":" + std::to_string(port());
}

uint16_t InetAddress::port() const { return ntohs(addr_.sin_port); }
