#include <unistd.h>

#include "EpollPoller.h"
#include <sys/eventfd.h>
#include <assert.h>
#include <iostream>

int main() {

    EpollPoller poller;
    std::vector<Channel *> activeChannels;

    int wakeupFd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

    Channel channel(wakeupFd);
    channel.enableReading();
    poller.updateChannel(&channel);

    uint64_t num = 112;
    write(wakeupFd, &num, sizeof(uint64_t));


    poller.poll(1000, &activeChannels);

    assert(activeChannels.size() == 1);
    assert(&channel == activeChannels[0]);
    assert(channel.revents() & EPOLLIN);

    num = 0;
    read(wakeupFd, &num, sizeof(uint64_t));
    std::cout << __FILE__ << ':' << __LINE__ << "  num: " <<  num << '\n';

    poller.removeChannel(&channel);
    close(wakeupFd);
}