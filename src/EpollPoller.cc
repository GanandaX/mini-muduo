#include "EpollPoller.h"

#include <assert.h>
#include <cstdlib>
#include <errno.h>
#include <iostream>
#include <string.h>
#include <unistd.h>

EpollPoller::EpollPoller()
    : epollFd_(epoll_create1(EPOLL_CLOEXEC)), events_(16) {
  assert(epollFd_ >= 0);
}

EpollPoller::~EpollPoller() {
  if (epollFd_ >= 0) {
    ::close(epollFd_);
  }
}

void EpollPoller::poll(int timeoutMs, std::vector<Channel *> *activeChannels) {
  int n = 0;
  do {
    n = ::epoll_wait(epollFd_, events_.data(), static_cast<int>(events_.size()),
                     timeoutMs);
  } while (n < 0 && errno == EINTR);

  if (n < 0) {
    const int savedErrno = errno;
    std::cerr << "epoll wait error: " << strerror(savedErrno) << '\n';
    std::abort();
  }

  if (n > 0) {
    fillActiveChannels(n, activeChannels);
  }

  if (n == static_cast<int>(events_.size())) {
    events_.resize(events_.size() << 1);
  }
}

void EpollPoller::updateChannel(Channel *channel) {
  assert(channel != nullptr);

  auto it = channels_.find(channel->fd());

  if (it != channels_.end()) {
    if (channel->events() == 0) {
      removeChannel(channel);
      return;
    }

    epoll_event event{};
    event.events = channel->events();
    event.data.ptr = channel;

    if (::epoll_ctl(epollFd_, EPOLL_CTL_MOD, channel->fd(), &event) < 0) {
      const int savedErrno = errno;
      std::cerr << "epoll mod error: " << strerror(savedErrno) << '\n';
      std::abort();
    }
    return;
  }

  if (channel->events() == 0) {
    return;
  }

  epoll_event event{};
  event.events = channel->events();
  event.data.ptr = channel;

  if (::epoll_ctl(epollFd_, EPOLL_CTL_ADD, channel->fd(), &event) < 0) {
    const int savedErrno = errno;
    std::cerr << "epoll add error: " << strerror(savedErrno) << '\n';
    std::abort();
  }

  channels_.emplace(channel->fd(), channel);
}

void EpollPoller::removeChannel(Channel *channel) {
  assert(channel != nullptr);

  auto it = channels_.find(channel->fd());
  if (it == channels_.end()) {
    return;
  }

  if (::epoll_ctl(epollFd_, EPOLL_CTL_DEL, channel->fd(), nullptr) < 0) {
    const int savedErrno = errno;
    std::cerr << "epoll del error: " << strerror(savedErrno) << '\n';
    std::abort();
  }

  channels_.erase(it);
}

void EpollPoller::fillActiveChannels(int eventCount,
                                     std::vector<Channel *> *activeChannels) {
  for (int i = 0; i < eventCount; i++) {
    Channel *channel = static_cast<Channel *>(events_[i].data.ptr);
    channel->setRevents(events_[i].events);
    activeChannels->push_back(channel);
  }
}
