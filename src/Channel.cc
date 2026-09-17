#include "Channel.h"
#include <iostream>

Channel::Channel(int fd) : fd_(fd), events_(0), revents_(0) {}

int Channel::fd() const { return fd_; }

uint32_t Channel::events() const { return events_; }

uint32_t Channel::revents() const { return revents_; }

void Channel::setRevents(uint32_t revents) { revents_ = revents; }

bool Channel::isReadingEnabled() const { return events_ & kReadEvents; }
bool Channel::isWritingEnabled() const { return events_ & EPOLLOUT; }

void Channel::enableReading() { events_ |= kReadEvents; }
void Channel::enableWriting() { events_ |= EPOLLOUT; }
void Channel::disableReading() { events_ &= ~kReadEvents; }
void Channel::disableWriting() { events_ &= ~EPOLLOUT; }
void Channel::disableAll() { events_ = 0; }

void Channel::setReadCallback(EventCallback cb) { readCallback_ = cb; }
void Channel::setWriteCallback(EventCallback cb) { writeCallback_ = cb; }
void Channel::setCloseCallback(EventCallback cb) { closeCallback_ = cb; }
void Channel::setErrorCallback(EventCallback cb) { errorCallback_ = cb; }

void Channel::handleEvent() {
  if (tied_) {
    std::shared_ptr<void> guard = tie_.lock();
    if (guard) {
      handleEventWithGuard();
    }
  } else {
    handleEventWithGuard();
  }
}

void Channel::tie(const std::shared_ptr<void> &owner) {
  tie_ = owner;
  tied_ = true;
}

void Channel::handleEventWithGuard() {
  if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
    if (closeCallback_) {
      closeCallback_();
    }
  }

  if (revents_ & EPOLLERR) {
    if (errorCallback_) {
      errorCallback_();
    }
  }

  if (revents_ & kReadEvents) {
    if (readCallback_) {
      readCallback_();
    }
  }

  if (revents_ & EPOLLOUT) {
    if (writeCallback_) {
      writeCallback_();
    }
  }
}