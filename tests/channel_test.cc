#include "Channel.h"
#include <assert.h>
#include <sys/epoll.h>
#include <iostream>

void base_test() {
  Channel channel(12);

  assert(channel.fd() == 12);
  assert(!channel.events());
  assert(!channel.revents());

  channel.enableReading();
  assert(channel.isReadingEnabled());
  assert(!channel.isWritingEnabled());

  channel.enableWriting();
  assert(channel.isReadingEnabled());
  assert(channel.isWritingEnabled());

  channel.disableWriting();
  assert(!channel.isWritingEnabled());
  assert(channel.isReadingEnabled());

  channel.disableAll();
  assert(!channel.isReadingEnabled());
  assert(!channel.isWritingEnabled());

  assert(channel.events() == 0);
  channel.setRevents(EPOLLIN | EPOLLOUT);
  assert(channel.revents() == (EPOLLIN | EPOLLOUT));
}

void callback_test() {
  int reads = 0;
  int writes = 0;

  Channel channel(10);

  channel.setReadCallback([&]() { ++reads; });
  channel.setWriteCallback([&]() { ++writes; });

  channel.setRevents(EPOLLIN);
  channel.handleEvent();
  assert(reads == 1);

  channel.setRevents(EPOLLOUT);
  channel.handleEvent();
  assert(writes == 1);
}

void tie_test() {

  Channel channel(12);

  uint16_t counter = 0;
  channel.setReadCallback([&]() { ++counter; });

  std::shared_ptr<uint16_t> shared_uint16_t = std::make_shared<uint16_t>(11);
  channel.tie(shared_uint16_t);

  channel.setRevents(EPOLLIN);
  channel.handleEvent();
  assert(counter == 1);

  shared_uint16_t.reset();

  channel.handleEvent();
  assert(counter == 1);

  std::cout << "after counter = " << counter << std::endl;
}

int main() {

  // base_test();
  // callback_test();
  tie_test();
}
