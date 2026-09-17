#ifndef MINI_MUDUO_BUFFER_H
#define MINI_MUDUO_BUFFER_H

#include <vector>

class Buffer {
public:
  explicit Buffer();

  size_t readableBytes() const;
  const char *peek() const;
  void append(const char *data, size_t len);
  void retrieve(size_t len);
  size_t writableBytes() const;
  void ensureWritableBytes(size_t len);
  void makeSpace(size_t len);

private:
  std::vector<char> buffer_;
  size_t readerIndex_;
  size_t writerIndex_;
};

#endif