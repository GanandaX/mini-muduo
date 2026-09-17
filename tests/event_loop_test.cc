#include <iostream>
#include <unistd.h>
#include <sys/eventfd.h>
#include <assert.h>

#include "EventLoop.h"

int main() {
    EventLoop loop;

    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    Channel channel(fd);

    channel.setReadCallback([&]() {
        uint64_t val;
        int n = 0;
        while (true) {
            n = read(fd, &val, sizeof(uint64_t));
            if (n == 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }

            std::cout << "val: " << val << std::endl;
            assert(val == 1122);
        }

        loop.quit();
    });
    channel.enableReading();
    loop.updateChannel(&channel);

    uint64_t num = 1122;
    write(fd, (void *) &num, sizeof(uint64_t));

    loop.loop();
    loop.removeChannel(&channel);
    close(fd);
}
