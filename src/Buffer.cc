#include "Buffer.h"
#include <algorithm>
#include <assert.h>
#include <cstring>

Buffer::Buffer() : buffer_(4096), readerIndex_(0), writerIndex_(0) {}

size_t Buffer::readableBytes() const { return writerIndex_ - readerIndex_; }

const char *Buffer::peek() const { return buffer_.data() + readerIndex_; }

void Buffer::append(const char *data, size_t len) {
  ensureWritableBytes(len);
  std::copy(data, data + len, buffer_.data() + writerIndex_);
  writerIndex_ += len;
}

void Buffer::retrieve(size_t len) {
  assert(len <= readableBytes());

  if (len < readableBytes()) {
    readerIndex_ += len;
  } else {
    readerIndex_ = 0;
    writerIndex_ = 0;
  }
}

size_t Buffer::writableBytes() const { return buffer_.size() - writerIndex_; }

void Buffer::ensureWritableBytes(size_t len) {
  if (writableBytes() < len) {
    makeSpace(len);
  }
}

void Buffer::makeSpace(size_t len) {
  if (writableBytes() >= len) {
    return;
  }

  if (readerIndex_ + writableBytes() >= len) {
    size_t readable = readableBytes();
    std::memmove(buffer_.data(), buffer_.data() + readerIndex_, readable);
    readerIndex_ = 0;
    writerIndex_ = readable;
  }

  while (writableBytes() < len) {
    buffer_.resize(buffer_.size() << 1);
  }
}