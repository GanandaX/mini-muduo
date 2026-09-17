#include "InetAddress.h"
#include <assert.h>
#include <iostream>

void base_test() {
  InetAddress addr(2007);

  std::cout << addr.toIpPort() << std::endl;
  assert(addr.port() == 2007);
  assert(addr.toIp() == "0.0.0.0");
  assert(addr.toIpPort() == "0.0.0.0:2007");
}

void local_loop_test() {
  InetAddress addr("127.0.0.1", 2007);

  std::cout << addr.toIpPort() << std::endl;
  assert(addr.port() == 2007);
  assert(addr.toIp() == "127.0.0.1");
  assert(addr.toIpPort() == "127.0.0.1:2007");
}

int main() { local_loop_test(); }