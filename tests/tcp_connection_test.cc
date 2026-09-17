#include <algorithm>
#include <array>
#include <assert.h>
#include <future>
#include <iostream>
#include <poll.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "EventLoopThread.h"
#include "TcpConnection.h"

void handle_read_test() {
  int fds[2];
  socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds);

  EventLoop loop;

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *buf) {
    std::cout << "readed msg: "
              << std::string(buf->peek(), buf->readableBytes()) << std::endl;
    assert(std::string(buf->peek(), buf->readableBytes()) == "hello");
    buf->retrieve(buf->readableBytes());
  });

  conn->connectEstablished();

  assert(write(fds[1], "hello", 5) == 5);

  loop.loop();
  conn->connectDestroyed();
  close(fds[1]);
}

void handle_close_test() {
  int fds[2];

  socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds);
  EventLoop loop;

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *buf) {
    std::cout << "msg: " << std::string(buf->peek(), buf->readableBytes())
              << std::endl;
    assert(std::string(buf->peek(), buf->readableBytes()) ==
           "hello_close_test");
    buf->retrieve(buf->readableBytes());
  });

  bool closed = false;
  conn->setCloseCallback([&](const TcpConnectionPtr &) {
    closed = true;
    loop.quit();
  });
  conn->connectEstablished();

  assert(write(fds[1], "hello_close_test", 16) == 16);
  shutdown(fds[1], SHUT_WR);
  loop.loop();

  conn->connectDestroyed();
  close(fds[1]);
  assert(closed);
}

void handle_half_read_test() {
  int fds[2];

  const char *msg = "handle_write_test";
  const int len = strlen(msg);

  socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds);
  EventLoop loop;

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *buf) {
    std::cout << "receive msg: "
              << std::string(buf->peek(), buf->readableBytes()) << std::endl;
    assert(std::string(buf->peek(), buf->readableBytes()) == msg);

    conn->send(buf->peek(), buf->readableBytes());
    buf->retrieve(buf->readableBytes());
    loop.quit();
  });

  conn->connectEstablished();

  assert(write(fds[1], msg, len) == len);
  shutdown(fds[1], SHUT_WR);
  loop.loop();

  char fds_1_buf[1024] = {0};
  read(fds[1], fds_1_buf, 1024);
  std::cout << "fds[1] readed: " << fds_1_buf << std::endl;

  conn->connectDestroyed();
  close(fds[1]);
}

void handle_send_test() {
  int fds[2];

  const char *msg = "handle_write_test";
  const int len = strlen(msg);

  socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds);
  EventLoop loop;

  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *buf) {
    std::cout << "receive msg: "
              << std::string(buf->peek(), buf->readableBytes()) << std::endl;
    assert(std::string(buf->peek(), buf->readableBytes()) == msg);

    conn->send(buf->peek(), buf->readableBytes());
    buf->retrieve(buf->readableBytes());
    loop.quit();
  });

  conn->connectEstablished();
  // conn->send()

  assert(write(fds[1], msg, len) == len);
  shutdown(fds[1], SHUT_WR);
  loop.loop();

  char fds_1_buf[1024] = {0};
  read(fds[1], fds_1_buf, 1024);
  std::cout << "fds[1] readed: " << fds_1_buf << std::endl;

  conn->connectDestroyed();
  close(fds[1]);
}

size_t fill_data(int fd) {
  size_t total_write = 0;
  std::string msg("c", 1024);

  ssize_t n = 0;
  while (true) {
    n = write(fd, msg.data(), msg.length());

    if (n > 0) {
      total_write += n;
      continue;
    }

    if (n == 0) {
      break;
    }

    if (errno == EINTR) {
      continue;
    }

    break;
  }

  return total_write;
}

void drain(int fd, size_t total_size) {

  std::array<char, 64 * 1024> data{};

  while (total_size > 0) {
    ssize_t n = read(fd, data.data(), data.size());

    if (n > 0) {
      total_size -= n;
      continue;
    }
    assert(false);
  }
}

void write_complete_test() {

  int fds[2];
  socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds);

  EventLoop loop;
  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setWriteCompleteCallback(
      [&](const TcpConnectionPtr &) { loop.quit(); });
  conn->connectEstablished();

  size_t total_filled = fill_data(fds[0]);
  std::cout << "filled " << total_filled << " bytes" << std::endl;
  conn->send("end", 3);
  drain(fds[1], total_filled);

  loop.loop();

  char rcv_buf[12] = {0};
  ssize_t rcv_cnt = read(fds[1], rcv_buf, 12);
  assert(rcv_cnt == 3);
  assert(std::string(rcv_buf, rcv_cnt) == "end");
  std::cout << "readed : " << rcv_buf << std::endl;

  conn->connectDestroyed();
  close(fds[1]);
}

void high_water_mark_notifies_once_while_buffer_remains_high_test() {
  int fds[2];
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0,
                      fds) == 0);

  std::promise<void> startReadPromise;
  auto startReadFuture = startReadPromise.get_future();
  std::promise<void> shouldClosePromise;
  auto shouldCloseFuture = shouldClosePromise.get_future();
  std::promise<void> couldCloseAssertPromise;
  auto couldCloseAssertFuture = couldCloseAssertPromise.get_future();

  const size_t highWaterMark = 10;
  size_t socketWriteBufferBytes = 0;
  const size_t queueWriteLen = 2 * highWaterMark;

  int conncetCnt = 0;
  int disconnectCnt = 0;
  int connectCloseCnt = 0;
  int highWaterMarkCnt = 0;
  int peerHalfCloseCnt = 0;
  int writeCompleteCnt = 0;

  size_t highWaterMarkCallbackBufferBytes = 0;

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(loop, fds[0]);
  conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++conncetCnt;
    } else {
      ++disconnectCnt;
    }
  });
  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++connectCloseCnt;
    conn->connectDestroyed();
    if (connectCloseCnt == 1) {
      couldCloseAssertPromise.set_value();
    }
  });
  conn->setHighWaterMarkCallback(
      [&](const TcpConnectionPtr &conn, size_t bufferedBytes) {
        ++highWaterMarkCnt;
        highWaterMarkCallbackBufferBytes = bufferedBytes;

        if (highWaterMarkCnt == 1) {
          startReadPromise.set_value();
        }
      },
      highWaterMark);
  conn->setLowWaterMarkCallback(nullptr, highWaterMark / 2);
  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerHalfCloseCnt;
    conn->shutdown();
  });
  conn->setWriteCompleteCallback([&](const TcpConnectionPtr &conn) {
    ++writeCompleteCnt;
    if (writeCompleteCnt == 1) {
      shouldClosePromise.set_value();
    }
  });

  loop->queueInLoop([&]() {
    socketWriteBufferBytes = 0;
    char buffer[1024];
    while (true) {
      ssize_t ret = ::write(fds[0], buffer, sizeof(buffer));
      if (ret > 0) {
        socketWriteBufferBytes += ret;
        continue;
      }

      if (ret < 0 && (errno == EINTR)) {
        continue;
      }

      if (ret < 0 && ((errno == EAGAIN) || (errno == EWOULDBLOCK))) {
        break;
      }

      std::cerr << "写满socket异常" << std::endl;
      std::abort();
    }
  });
  loop->queueInLoop([&]() { conn->connectEstablished(); });
  loop->queueInLoop([&]() {
    char buffer[1024];
    conn->send(buffer, highWaterMark - 1);
    conn->send(buffer, 1);
    conn->send(buffer, highWaterMark);
  });

  auto waitForRes = startReadFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for 超时" << std::endl;
    std::abort();
  }

  size_t expectedTotal = socketWriteBufferBytes + 2 * highWaterMark;

  size_t readedLen = 0;
  char buffer[1024];
  ssize_t ret;
  while (readedLen < expectedTotal) {
    ret = ::read(fds[1], buffer,
                 std::min(sizeof(buffer), expectedTotal - readedLen));
    if (ret > 0) {
      readedLen += ret;
      continue;
    }

    if (ret < 0 && (errno == EINTR)) {
      continue;
    }

    if (ret < 0 && ((errno == EAGAIN) || (errno == EWOULDBLOCK))) {
      pollfd pfd;
      pfd.events = POLLIN;
      pfd.fd = fds[1];
      int ret = poll(&pfd, 1, 1000);
      if (ret > 0) {
        continue;
      }
      std::cerr << "读数据有误或超时" << std::endl;
      std::abort();
    }

    std::cerr << "对端异常" << std::endl;
    std::abort();
  }

  waitForRes = shouldCloseFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for 超时" << std::endl;
    std::abort();
  }

  close(fds[1]);

  waitForRes = couldCloseAssertFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for 超时" << std::endl;
    std::abort();
  }

  assert(conncetCnt == 1);
  assert(disconnectCnt == 1);
  assert(connectCloseCnt == 1);
  assert(highWaterMarkCnt == 1);
  assert(writeCompleteCnt == 1);
  assert(highWaterMarkCallbackBufferBytes == highWaterMark);
  assert(readedLen == (socketWriteBufferBytes + queueWriteLen));
}

void high_water_mark_notifies_again_after_buffer_drains_test() {
  int fds[2];
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0,
                      fds) == 0);

  std::mutex mutex;
  std::condition_variable startReadConditionVariable;
  std::condition_variable couldShutdownWriteConditionVariable;
  std::condition_variable couldCloseConditionVariable;
  std::condition_variable couldCloseAssertConditionVariable;

  const size_t highWaterMark = 10;
  size_t socketWriteBufferBytes = 0;

  std::atomic<int> conncetCnt = 0;
  std::atomic<int> disconnectCnt = 0;
  std::atomic<int> connectCloseCnt = 0;
  std::atomic<int> highWaterMarkCnt = 0;
  std::atomic<int> peerHalfCloseCnt = 0;
  std::atomic<int> writeCompleteCnt = 0;

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(loop, fds[0]);
  conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++conncetCnt;
    } else {
      ++disconnectCnt;
    }
  });
  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    conn->connectDestroyed();
    {
      std::unique_lock<std::mutex> locker(mutex);
      ++connectCloseCnt;
    }
    couldCloseAssertConditionVariable.notify_one();
  });
  conn->setHighWaterMarkCallback(
      [&](const TcpConnectionPtr &conn, size_t bufferedBytes) {
        {
          std::unique_lock<std::mutex> locker(mutex);
          assert(bufferedBytes == highWaterMark);
          ++highWaterMarkCnt;
        }
        startReadConditionVariable.notify_one();
      },
      highWaterMark);
  conn->setLowWaterMarkCallback(nullptr, highWaterMark / 2);

  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    conn->shutdown();
    ++peerHalfCloseCnt;
    couldCloseConditionVariable.notify_one();
  });
  conn->setWriteCompleteCallback([&](const TcpConnectionPtr &conn) {
    {
      std::unique_lock<std::mutex> locker(mutex);
      ++writeCompleteCnt;
    }
    couldShutdownWriteConditionVariable.notify_one();
  });

  loop->queueInLoop([&]() { conn->connectEstablished(); });

  for (size_t i = 0; i < 2; ++i) {
    loop->queueInLoop([&]() {
      socketWriteBufferBytes = 0;
      char buffer[1024];
      while (true) {
        ssize_t ret = ::write(fds[0], buffer, sizeof(buffer));
        if (ret > 0) {
          socketWriteBufferBytes += ret;
          continue;
        }

        if (ret < 0 && (errno == EINTR)) {
          continue;
        }

        if (ret < 0 && ((errno == EAGAIN) || (errno == EWOULDBLOCK))) {
          break;
        }

        std::cerr << "写满socket异常" << std::endl;
        std::abort();
      }
    });
    loop->queueInLoop([&]() {
      char buffer[1024];
      conn->send(buffer, highWaterMark - 1);
      conn->send(buffer, 1);
      conn->send(buffer, highWaterMark);
    });

    {
      std::unique_lock<std::mutex> locker(mutex);
      bool waitForRes = startReadConditionVariable.wait_for(
          locker, std::chrono::seconds(1),
          [&] { return highWaterMarkCnt >= i + 1; });

      if (waitForRes == false) {
        std::cerr << "wait for 超时" << std::endl;
        std::abort();
      }
    }

    size_t expectedTotal = socketWriteBufferBytes + 2 * highWaterMark;

    size_t readedLen = 0;
    char buffer[1024];
    ssize_t ret;
    while (readedLen < expectedTotal) {
      ret = ::read(fds[1], buffer,
                   std::min(sizeof(buffer), expectedTotal - readedLen));
      if (ret > 0) {
        readedLen += ret;
        continue;
      }

      if (ret < 0 && (errno == EINTR)) {
        continue;
      }

      if (ret < 0 && ((errno == EAGAIN) || (errno == EWOULDBLOCK))) {
        pollfd pfd{};
        pfd.events = POLLIN;
        pfd.fd = fds[1];
        int ret = poll(&pfd, 1, 1000);
        if (ret > 0) {
          continue;
        }
        std::cerr << "读数据有误或超时" << std::endl;
        std::abort();
      }

      std::cerr << "对端异常" << std::endl;
      std::abort();
    }

    {
      std::unique_lock<std::mutex> locker(mutex);
      bool waitForRes = couldShutdownWriteConditionVariable.wait_for(
          locker, std::chrono::seconds(1),
          [&] { return writeCompleteCnt >= i + 1; });

      if (waitForRes == false) {
        std::cerr << "wait for 超时" << std::endl;
        std::abort();
      }
    }
  }

  ::shutdown(fds[1], SHUT_WR);

  {
    std::unique_lock<std::mutex> locker(mutex);
    bool waitForRes = couldCloseConditionVariable.wait_for(
        locker, std::chrono::seconds(1), [&] { return peerHalfCloseCnt >= 1; });

    if (waitForRes == false) {
      std::cerr << "wait for 超时" << std::endl;
      std::abort();
    }
  }

  close(fds[1]);

  {
    std::unique_lock<std::mutex> locker(mutex);
    bool waitForRes = couldCloseAssertConditionVariable.wait_for(
        locker, std::chrono::seconds(1), [&] { return connectCloseCnt >= 1; });

    if (waitForRes == false) {
      std::cerr << "wait for 超时" << std::endl;
      std::abort();
    }
  }

  assert(conncetCnt == 1);
  assert(disconnectCnt == 1);
  assert(connectCloseCnt == 1);
  assert(highWaterMarkCnt == 2);
  assert(writeCompleteCnt == 2);
}

void low_water_mark_notifies_before_write_complete_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  constexpr size_t LowerWaterMark = 10;

  int connectCnt = 0;
  int disconnectCnt = 0;
  int closeCnt = 0;
  int peerCloseCnt = 0;
  int writeCompleteCnt = 0;
  int lowWaterCnt = 0;

  std::promise<void> couldClosePromise;
  auto couldCloseFuture = couldClosePromise.get_future();
  std::promise<void> couldAssertPromise;
  auto couldAssertFuture = couldAssertPromise.get_future();

  size_t writeCallCount = 0;
  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(
      loop, fds[0], [&](int, const void *, size_t n) -> ssize_t {
        switch (++writeCallCount) {
        case 1:
          assert(n == LowerWaterMark + 1);
          errno = EAGAIN;
          return -1;
        case 2:
          assert(n == LowerWaterMark + 1);
          return 1;
        case 3:
          assert(n == LowerWaterMark);
          errno = EAGAIN;
          return -1;
        case 4:
          assert(n == LowerWaterMark);
          return static_cast<ssize_t>(n);
        default:
          std::cerr << "unexpected write" << std::endl;
          std::abort();
        }
      });

  conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connectCnt;
    } else {
      ++disconnectCnt;
    }
  });
  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++closeCnt;
    conn->connectDestroyed();

    if (closeCnt == 1) {
      couldAssertPromise.set_value();
    }
  });
  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerCloseCnt;
    conn->shutdown();
  });
  conn->setWriteCompleteCallback([&](const TcpConnectionPtr &conn) {
    std::cout << "write complete invoked" << std::endl;
    ++writeCompleteCnt;
    assert(lowWaterCnt == 1);

    if (writeCompleteCnt == 1) {
      couldClosePromise.set_value();
    }
  });
  conn->setLowWaterMarkCallback(
      [&](const TcpConnectionPtr &conn, size_t bufferedBytes) {
        std::cout << "low water callback invoked" << std::endl;
        ++lowWaterCnt;
        assert(bufferedBytes == LowerWaterMark);
        assert(!writeCompleteCnt);
      },
      LowerWaterMark);

  loop->queueInLoop([conn = conn]() { conn->connectEstablished(); });

  loop->queueInLoop([conn = conn]() {
    char buffer[1024];
    conn->send(buffer, LowerWaterMark + 1);
  });

  auto waitForRes = couldCloseFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << " wait for timeout" << std::endl;
    std::abort();
  }

  close(fds[1]);

  waitForRes = couldAssertFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << " wait for timeout" << std::endl;
    std::abort();
  }

  assert(connectCnt == 1);
  assert(disconnectCnt == 1);
  assert(closeCnt == 1);
  assert(peerCloseCnt == 1);
  assert(writeCompleteCnt == 1);
  assert(lowWaterCnt == 1);
  assert(writeCallCount == 4);
}

int main() {
  // handle_read_test();
  // handle_close_test();
  // handle_half_read_test();
  // write_complete_test();
  high_water_mark_notifies_once_while_buffer_remains_high_test();
  high_water_mark_notifies_again_after_buffer_drains_test();
  // low_water_mark_notifies_before_write_complete_test();
}