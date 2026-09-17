#include "Socket.h"
#include <assert.h>
#include <sys/socket.h>
#include <unistd.h>

void base_test() {

  Socket sock(socket(AF_INET, SOCK_STREAM, 0));

  InetAddress addr(2007);

  sock.bindAddress(addr);
  sock.listen();

  InetAddress peerAddr(0);
  int clientFd = sock.accept(&peerAddr);

  assert(clientFd >= 0);

  Socket clientSocket(clientFd);
  clientSocket.shutdownWrite();
}

int main() { base_test(); }