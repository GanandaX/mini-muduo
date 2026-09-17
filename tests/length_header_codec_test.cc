#include "LengthHeaderCodec.h"
#include "TcpClient.h"
#include "TcpServer.h"

#include <arpa/inet.h>
#include <assert.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

void complete_frame_test() {
  LengthHeaderCodec lengthHeaderCodec;

  const size_t kHeaderLen = sizeof(uint32_t);
  char msg[] = "hello world!";
  size_t msgLen = strlen(msg);
  const size_t packetLen = kHeaderLen + msgLen;
  uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

  std::string packet;
  packet.resize(packetLen);

  ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
  ::memcpy(packet.data() + kHeaderLen, msg, msgLen);

  Buffer buffer;
  buffer.append(packet.data(), packetLen);

  bool messageCallbacked = false;

  lengthHeaderCodec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &callbackMsg) {
        messageCallbacked = true;
        assert(callbackMsg == msg);
        std::cout << "len(" << callbackMsg.size() << "):" << callbackMsg
                  << std::endl;
      });

  lengthHeaderCodec.onMessage(nullptr, &buffer);

  assert(messageCallbacked);
  assert(buffer.readableBytes() == 0);
}

void partial_header_test() {
  LengthHeaderCodec lengthHeaderCodec;

  const size_t kHeaderLen = sizeof(uint32_t);
  char msg[] = "hello world!";
  size_t msgLen = strlen(msg);
  const size_t packetLen = kHeaderLen + msgLen;
  uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

  std::string packet;
  packet.resize(packetLen);

  ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
  ::memcpy(packet.data() + kHeaderLen, msg, msgLen);

  int messageCallbackedCnt = 0;

  lengthHeaderCodec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &callbackMsg) {
        ++messageCallbackedCnt;
        assert(callbackMsg == msg);
        std::cout << "len(" << callbackMsg.size() << "):" << callbackMsg
                  << std::endl;
      });

  size_t firstWriteLen = 3;
  Buffer buffer;
  buffer.append(packet.data(), firstWriteLen);
  lengthHeaderCodec.onMessage(nullptr, &buffer);
  assert(buffer.readableBytes() == firstWriteLen);
  assert(messageCallbackedCnt == 0);

  buffer.append(packet.data() + firstWriteLen, packetLen - firstWriteLen);
  lengthHeaderCodec.onMessage(nullptr, &buffer);

  assert(messageCallbackedCnt == 1);
  assert(buffer.readableBytes() == 0);
}

void partial_body_test() {
  LengthHeaderCodec lengthHeaderCodec;

  const size_t kHeaderLen = sizeof(uint32_t);
  char msg[] = "hello world!";
  size_t msgLen = strlen(msg);
  const size_t packetLen = kHeaderLen + msgLen;
  uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

  std::string packet;
  packet.resize(packetLen);

  ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
  ::memcpy(packet.data() + kHeaderLen, msg, msgLen);

  int messageCallbackedCnt = 0;

  lengthHeaderCodec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &callbackMsg) {
        ++messageCallbackedCnt;
        assert(callbackMsg == msg);
        std::cout << "len(" << callbackMsg.size() << "):" << callbackMsg
                  << std::endl;
      });

  size_t firstWriteLen = 5;
  Buffer buffer;
  buffer.append(packet.data(), firstWriteLen);
  lengthHeaderCodec.onMessage(nullptr, &buffer);
  assert(buffer.readableBytes() == firstWriteLen);
  assert(messageCallbackedCnt == 0);

  buffer.append(packet.data() + firstWriteLen, packetLen - firstWriteLen);
  lengthHeaderCodec.onMessage(nullptr, &buffer);

  assert(messageCallbackedCnt == 1);
  assert(buffer.readableBytes() == 0);
}

void multiple_frames_test() {
  LengthHeaderCodec lengthHeaderCodec;

  const uint8_t kHeaderLen = sizeof(uint32_t);
  std::vector<std::string> readMsg;
  std::vector<std::string> sendMsg;

  int messageCallbackedCnt = 0;

  lengthHeaderCodec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &callbackMsg) {
        ++messageCallbackedCnt;
        readMsg.push_back(callbackMsg);
        std::cout << "readed: " << callbackMsg << std::endl;
      });

  sendMsg.push_back("hello world! first");
  sendMsg.push_back("hello world! second");

  std::string sendPacket;

  for (auto &str : sendMsg) {
    size_t msgLen = str.length();
    const size_t packetLen = kHeaderLen + msgLen;
    uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

    std::string packet;
    packet.resize(packetLen);

    ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
    ::memcpy(packet.data() + kHeaderLen, str.data(), msgLen);

    sendPacket.append(packet);
  }

  Buffer buffer;
  buffer.append(sendPacket.data(), sendPacket.length());
  lengthHeaderCodec.onMessage(nullptr, &buffer);

  assert(buffer.readableBytes() == 0);

  assert(messageCallbackedCnt == sendMsg.size());
  for (size_t i = 0; i < readMsg.size(); ++i) {
    assert(sendMsg[i] == readMsg[i]);
  }
}

void empty_frame_test() {
  LengthHeaderCodec lengthHeaderCodec;

  const size_t kHeaderLen = sizeof(uint32_t);
  std::vector<std::string> readMsg;
  std::vector<std::string> sendMsg;

  int messageCallbackedCnt = 0;

  lengthHeaderCodec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &callbackMsg) {
        ++messageCallbackedCnt;
        readMsg.push_back(callbackMsg);
        std::cout << "readed: " << callbackMsg << std::endl;
      });

  sendMsg.push_back("");

  std::string sendPacket;

  for (auto &str : sendMsg) {
    size_t msgLen = str.length();
    const size_t packetLen = kHeaderLen + msgLen;
    uint32_t networkMsgLen = htonl(static_cast<uint32_t>(msgLen));

    std::string packet;
    packet.resize(packetLen);

    ::memcpy(packet.data(), &networkMsgLen, kHeaderLen);
    ::memcpy(packet.data() + kHeaderLen, str.data(), msgLen);

    sendPacket.append(packet);
  }

  Buffer buffer;
  buffer.append(sendPacket.data(), sendPacket.length());
  lengthHeaderCodec.onMessage(nullptr, &buffer);

  assert(buffer.readableBytes() == 0);

  assert(messageCallbackedCnt == sendMsg.size());
  for (size_t i = 0; i < readMsg.size(); ++i) {
    assert(sendMsg[i] == readMsg[i]);
  }
}

ssize_t readFully(int fd, void *buf, size_t msgLen) {
  ssize_t readedLen = 0;
  char *data = static_cast<char *>(buf);

  while (readedLen != msgLen) {
    ssize_t ret = ::read(fd, data + readedLen, msgLen - readedLen);

    if (ret < 0) {
      if (errno == EINTR) {
        continue;
      }
      readedLen = 0;
      return -1;
    }

    if (ret > 0) {
      readedLen += ret;
    } else {
      break;
    }
  }

  return readedLen;
}

std::string generateString(bool isOverMaxSize) {
  static constexpr size_t kMaxMessageSize = 64 << 20;

  if (isOverMaxSize) {
    return std::string(kMaxMessageSize + 1, 'A');
  }

  // 固定 seed：每次测试运行得到相同随机序列，失败可复现。
  static std::mt19937 generator(12345);

  // 正常随机测试无需生成几十 MiB，0~4096 已足够覆盖多种长度。
  std::uniform_int_distribution<size_t> lengthDistribution(0, 4096);
  std::uniform_int_distribution<int> charDistribution('A', 'Z');

  const size_t length = lengthDistribution(generator);
  std::string result(length, '\0');

  for (size_t i = 0; i < length; ++i) {
    result[i] = static_cast<char>(charDistribution(generator));
  }

  return result;
}

void complete_frame_read_test() {
  int fds[2];
  assert(0 == ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           0, fds));

  int flag = fcntl(fds[1], F_GETFL);
  assert(0 == fcntl(fds[1], F_SETFL, flag & ~SOCK_NONBLOCK));

  EventLoop loop;
  TcpConnectionPtr conn = std::make_shared<TcpConnection>(&loop, fds[0]);
  conn->connectEstablished();

  LengthHeaderCodec codec;
  std::vector<std::string> sendMsg;
  sendMsg.push_back("hello world first");
  sendMsg.push_back("hello world second");

  char msg[] = "hello world";
  size_t msgLen = strlen(msg);
  codec.send(conn, "hello world", msgLen);

  /*
    pollfd pfd;
    pfd.fd = fds[1];
    pfd.events = POLLIN;
    assert(0 == ::poll(&pfd, 1, 100)); */

  const size_t kHeaderLen = sizeof(uint32_t);
  uint32_t netMsgLen = 0;
  readFully(fds[1], &netMsgLen, kHeaderLen);
  uint32_t receMsgLen = ntohl(netMsgLen);

  assert(msgLen == receMsgLen);

  std::string readedMsg;
  readedMsg.resize(receMsgLen);

  uint32_t readedLen = 0;
  readFully(fds[1], readedMsg.data(), msgLen);

  assert(readedMsg == msg);

  conn->connectDestroyed();
  close(fds[1]);
}

void multiple_frames_read_test() {
  int fds[2];
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);

  int flag = ::fcntl(fds[0], F_GETFL);
  assert(0 == ::fcntl(fds[0], F_SETFL, flag | O_NONBLOCK));

  EventLoop loop;
  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);
  conn->connectEstablished();

  LengthHeaderCodec codec;

  std::vector<std::string> sendMsg;
  std::vector<std::string> recvMsg;
  sendMsg.push_back("hello world first");
  sendMsg.push_back("hello world second");

  for (auto &str : sendMsg) {
    codec.send(conn, str.data(), str.length());
  }

  for (size_t i = 0; i < sendMsg.size(); ++i) {
    uint32_t netMsgLen = 0;
    uint32_t msgLen = 0;
    const size_t kHeaderLen = sizeof(uint32_t);

    int ret = readFully(fds[1], &netMsgLen, kHeaderLen);
    if (ret < 0) {
      assert(false);
    }
    msgLen = ntohl(netMsgLen);
    assert(msgLen == sendMsg.at(i).length());

    std::string str;
    str.resize(msgLen);
    ret = readFully(fds[1], str.data(), msgLen);
    if (ret != msgLen) {
      assert(false);
    }

    assert(str == sendMsg.at(i));
    recvMsg.push_back(std::move(str));
  }

  assert(sendMsg.size() == recvMsg.size());

  conn->connectDestroyed();
  close(fds[1]);
}

void oversized_message_is_not_sent_test() {
  int fds[2];
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);

  int flag = ::fcntl(fds[0], F_GETFL);
  assert(flag >= 0);
  assert(0 == ::fcntl(fds[0], F_SETFL, flag | O_NONBLOCK));

  EventLoop loop;
  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);
  conn->connectEstablished();

  LengthHeaderCodec codec;

  std::vector<std::string> sendMsg;
  // sendMsg.push_back(std::string(64 << 20, 'A'));
  sendMsg.push_back(std::string(64 << 20 + 1, 'A'));
  // sendMsg.push_back(std::move(generateString(true)));

  for (auto &str : sendMsg) {
    codec.send(conn, str.data(), str.length());
  }

  pollfd pfd{};
  pfd.fd = fds[1];
  pfd.events = POLLIN;
  assert(0 == ::poll(&pfd, 1, 0));
  // assert(::poll(&pfd, 1, 0));

  conn->connectDestroyed();
  close(fds[1]);
}

void empty_frame_sent_test() {
  int fds[2];
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);

  int flag = ::fcntl(fds[0], F_GETFL);
  assert(flag >= 0);
  assert(0 == ::fcntl(fds[0], F_SETFL, flag | O_NONBLOCK));

  EventLoop loop;
  auto conn = std::make_shared<TcpConnection>(&loop, fds[0]);
  conn->connectEstablished();

  LengthHeaderCodec codec;
  std::vector<std::string> sendMsg;
  std::vector<std::string> recvMsg;
  sendMsg.push_back(std::string());

  for (auto &str : sendMsg) {
    codec.send(conn, str.data(), str.length());
  }

  for (size_t i = 0; i < sendMsg.size(); ++i) {
    uint32_t netMsgLen = 0;
    uint32_t msgLen = 0;
    const size_t kHeaderLen = sizeof(uint32_t);

    int ret = readFully(fds[1], &netMsgLen, kHeaderLen);
    if (ret < 0) {
      assert(false);
    }

    pollfd pfd{};
    pfd.fd = fds[1];
    pfd.events = POLLIN;
    assert(0 == ::poll(&pfd, 1, 0));

    msgLen = ntohl(netMsgLen);
    assert(msgLen == sendMsg.at(i).length());

    std::string str;
    str.resize(msgLen);
    ret = readFully(fds[1], str.data(), msgLen);
    if (ret != msgLen) {
      assert(false);
    }

    assert(str == sendMsg.at(i));
    recvMsg.push_back(std::move(str));
  }

  assert(sendMsg.size() == recvMsg.size());

  conn->connectDestroyed();
  close(fds[1]);
}

void echo_test() {
  int serverfd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverfd >= 0);
  int use = 1;
  assert(0 ==
         ::setsockopt(serverfd, SOCK_STREAM, SO_REUSEADDR, &use, sizeof(use)));
  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t serverLen = sizeof(sockaddr_in);
  assert(0 == ::bind(serverfd,
                     reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                     serverLen));
  assert(0 ==
         ::getsockname(serverfd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &serverLen));

  bool sendToServer = false;
  bool recvFromClient = false;
  bool recvFromServer = false;
  bool connectError = false;

  std::string echoTestMsg("hello world!");

  EventLoop loop;
  LengthHeaderCodec serverCodec;
  LengthHeaderCodec clientCodec;
  TcpServer server(&loop, serverfd);
  TcpClient client(&loop, serverAddr);

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage,
                                      &serverCodec, std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() { loop.quit(); });

  serverCodec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        recvFromClient = true;
        serverCodec.send(conn, msg.data(), msg.length());
        conn->shutdown();
      });

  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage,
                                      &clientCodec, std::placeholders::_1,
                                      std::placeholders::_2));

  clientCodec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        recvFromServer = true;
        assert(msg == echoTestMsg);
        client.disconnect();
      });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      sendToServer = true;
      clientCodec.send(conn, echoTestMsg.data(), echoTestMsg.length());
    } else {
      loop.queueInLoop([&]() { server.stop(std::chrono::milliseconds(1000)); });
    }
  });

  client.setConnectionErrorCallback([&](int) {
    connectError = true;
    loop.quit();
  });

  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    conn->shutdown();
  });

  loop.runAfter(std::chrono::milliseconds(2000), [] {
    std::cerr << "echo test timeout\n";
    std::abort();
  });

  server.start();
  client.connect();

  loop.loop();

  assert(sendToServer);
  assert(recvFromClient);
  assert(recvFromServer);
  assert(!connectError);
}

int main() {
  complete_frame_test();
  partial_header_test();
  partial_body_test();
  multiple_frames_test();
  complete_frame_read_test();
  multiple_frames_read_test();
  oversized_message_is_not_sent_test();
  empty_frame_test();
  empty_frame_sent_test();
  echo_test();
}