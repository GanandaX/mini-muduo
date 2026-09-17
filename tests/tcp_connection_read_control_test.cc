#include "EventLoopThread.h"
#include "TcpConnection.h"

#include <assert.h>
#include <future>
#include <iostream>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

void stop_read_defers_message_delivery_until_start_read_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  std::promise<void> stopReadAppliedPromise;
  auto stopReadAppliedFuture = stopReadAppliedPromise.get_future();
  std::promise<void> couldStartReadPromise;
  auto couldStartReadFuture = couldStartReadPromise.get_future();
  std::promise<void> messageReceivedPromise;
  auto couldCloseFuture = messageReceivedPromise.get_future();
  std::promise<void> couldAssertPromise;
  auto couldAssertFuture = couldAssertPromise.get_future();

  char msg[] = "hello world";
  size_t msgLen = strlen(msg);

  int connectCnt = 0;
  int disconnectCnt = 0;
  int closeCnt = 0;
  int peerCloseCnt = 0;
  int messageCnt = 0;

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(loop, fds[0]);

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
    couldAssertPromise.set_value();
  });
  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerCloseCnt;
    conn->shutdown();
  });
  conn->setMessageCallback([&](const TcpConnectionPtr &conn, Buffer *buffer) {
    ++messageCnt;
    assert(std::string(buffer->peek(), buffer->readableBytes()) ==
           std::string(msg, msgLen));
    buffer->retrieve(buffer->readableBytes());
    messageReceivedPromise.set_value();
  });

  loop->queueInLoop([&]() { conn->connectEstablished(); });
  conn->stopRead();

  loop->runAfter(std::chrono::milliseconds(100), [&]() {
    assert(messageCnt == 0);
    stopReadAppliedPromise.set_value();
  });

  auto waitForRes = stopReadAppliedFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ssize_t writtenBytes = ::write(fds[1], msg, msgLen);
  assert(writtenBytes == msgLen);

  loop->runAfter(std::chrono::milliseconds(100), [&]() {
    assert(messageCnt == 0);
    couldStartReadPromise.set_value();
  });

  waitForRes = couldStartReadFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  conn->startRead();

  waitForRes = couldCloseFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ::shutdown(fds[1], SHUT_WR);

  waitForRes = couldAssertFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ::close(fds[1]);

  assert(connectCnt == 1);
  assert(disconnectCnt == 1);
  assert(closeCnt == 1);
  assert(peerCloseCnt == 1);
  assert(messageCnt == 1);
}

void stop_read_defers_peer_half_close_until_start_read_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  std::promise<void> stopReadAppliedPromise;
  auto stopReadAppliedFuture = stopReadAppliedPromise.get_future();
  std::promise<void> couldStartReadPromise;
  auto couldStartReadFuture = couldStartReadPromise.get_future();
  std::promise<void> couldAssertPromise;
  auto couldAssertFuture = couldAssertPromise.get_future();

  int connectCnt = 0;
  int disconnectCnt = 0;
  int closeCnt = 0;
  int peerCloseCnt = 0;

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(loop, fds[0]);

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
    couldAssertPromise.set_value();
  });
  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerCloseCnt;
    conn->shutdown();
  });
  conn->setMessageCallback(
      [&](const TcpConnectionPtr &conn, Buffer *buffer) { assert(false); });

  loop->queueInLoop([&]() { conn->connectEstablished(); });
  conn->stopRead();

  loop->runAfter(std::chrono::milliseconds(100),
                 [&]() { stopReadAppliedPromise.set_value(); });

  auto waitForRes = stopReadAppliedFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ::shutdown(fds[1], SHUT_WR);

  loop->runAfter(std::chrono::milliseconds(100), [&]() {
    assert(peerCloseCnt == 0);
    couldStartReadPromise.set_value();
  });

  waitForRes = couldStartReadFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  conn->startRead();

  waitForRes = couldAssertFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ::close(fds[1]);

  assert(connectCnt == 1);
  assert(disconnectCnt == 1);
  assert(closeCnt == 1);
  assert(peerCloseCnt == 1);
}

void data_buffered_before_connection_established_is_delivered_after_establishment_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  std::promise<void> couldConnectEstablishedPromise;
  auto couldConnectEstablishedFuture =
      couldConnectEstablishedPromise.get_future();

  std::promise<void> couldShutWritePromise;
  auto couldShutWriteFuture = couldShutWritePromise.get_future();

  std::promise<void> couldAssertPromise;
  auto couldAssertFuture = couldAssertPromise.get_future();

  char msg[] = "hello world";
  size_t msgLen = strlen(msg);

  int connectCnt = 0;
  int disconnectCnt = 0;
  int messageCnt = 0;
  int closeCnt = 0;
  int peerCloseCnt = 0;

  EventLoopThread loopThread;
  auto loop = loopThread.startLoop();

  auto conn = std::make_shared<TcpConnection>(loop, fds[0]);

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
    couldAssertPromise.set_value();
  });
  conn->setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++peerCloseCnt;
    conn->shutdown();
  });
  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *buffer) {
    assert(std::string(buffer->peek(), buffer->readableBytes()) ==
           std::string(msg, msgLen));
    buffer->retrieve(buffer->readableBytes());
    ++messageCnt;
    couldShutWritePromise.set_value();
  });

  ssize_t writtenBytes = ::write(fds[1], msg, msgLen);
  assert(writtenBytes == msgLen);

  loop->runAfter(std::chrono::milliseconds(100),
                 [&]() { couldConnectEstablishedPromise.set_value(); });
  auto waitForRes =
      couldConnectEstablishedFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  loop->queueInLoop([&]() {
    assert(messageCnt == 0);
    conn->connectEstablished();
  });

  waitForRes = couldShutWriteFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }

  ::shutdown(fds[1], SHUT_WR);

  waitForRes = couldAssertFuture.wait_for(std::chrono::seconds(1));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for timeout" << std::endl;
    std::abort();
  }
  ::close(fds[1]);

  assert(connectCnt == 1);
  assert(disconnectCnt == 1);
  assert(closeCnt == 1);
  assert(peerCloseCnt == 1);
  assert(messageCnt == 1);
}

void stop_read_from_message_callback_stops_current_read_dispatch_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  int connCnt = 0;
  int disconnCnt = 0;
  int peerCloseCnt = 0;
  int messageCnt = 0;
  int closeCnt = 0;

  EventLoop loop;
  TcpConnectionPtr conn = std::make_shared<TcpConnection>(&loop, fds[0]);

  conn->setConnectionCallBack([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connCnt;
    } else {
      ++disconnCnt;
    }
  });
  conn->setMessageCallback([&](const TcpConnectionPtr &, Buffer *) {
    ++messageCnt;
    conn->stopRead();

    loop.queueInLoop([conn, &messageCnt] {
      assert(messageCnt == 1);
      conn->forceClose();
    });
  });
  conn->setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { ++peerCloseCnt; });

  conn->setCloseCallback([&](const TcpConnectionPtr &conn) {
    ++closeCnt;
    conn->connectDestroyed();
    loop.quit();
  });
  conn->connectEstablished();

  char buffer[1024] = {0};
  assert(::write(fds[1], buffer, sizeof(buffer)) ==
         static_cast<ssize_t>(sizeof(buffer)));
  assert(::write(fds[1], buffer, sizeof(buffer)) ==
         static_cast<ssize_t>(sizeof(buffer)));

  loop.loop();

  close(fds[1]);

  assert(connCnt == 1);
  assert(disconnCnt == 1);
  assert(peerCloseCnt == 0);
  assert(messageCnt == 1);
  assert(closeCnt == 1);
}

int main() {
  stop_read_defers_message_delivery_until_start_read_test();
  stop_read_defers_peer_half_close_until_start_read_test();
  data_buffered_before_connection_established_is_delivered_after_establishment_test();
  stop_read_from_message_callback_stops_current_read_dispatch_test();
}