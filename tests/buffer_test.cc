#include "Buffer.h"
#include <assert.h>
#include <string>
#include <iostream>

int main() {

  Buffer buffer;

  buffer.append("hello", 5);
  assert(buffer.readableBytes() == 5);
  assert(std::string(buffer.peek(), buffer.readableBytes()) == "hello");

  buffer.retrieve(2);
  assert(buffer.readableBytes() == 3);
  assert(std::string(buffer.peek(), buffer.readableBytes()) == "llo");

  // 触发前移与扩容；原有 "llo" 必须保留。
  std::string payload(5000, 'x');
  buffer.append(payload.data(), payload.size());

  assert(buffer.readableBytes() == 3 + payload.size());
  assert(std::string(buffer.peek(), 3) == "llo");
  assert(std::string(buffer.peek() + 3, payload.size()) == payload);

  buffer.retrieve(buffer.readableBytes());
  assert(buffer.readableBytes() == 0);

  std::cout << "Buffer tests passed\n";
}