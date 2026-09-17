#ifndef MINI_MUDUO_EPOLLPOLLER_H
#define MINI_MUDUO_EPOLLPOLLER_H

#include "Channel.h"
#include <map>
#include <vector>
#include <sys/epoll.h>

class EpollPoller {

public:
  explicit EpollPoller();

  ~EpollPoller();

  void poll(int timeoutMs, std::vector<Channel *> *activeChannels);

  void updateChannel(Channel *channel);
  void removeChannel(Channel *channel);

private:
  void fillActiveChannels(int eventCount,
                          std::vector<Channel *> *activeChannels);

  int epollFd_;
  std::vector<epoll_event> events_;
  std::map<int, Channel *> channels_;
};

#endif
