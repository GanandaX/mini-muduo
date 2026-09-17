import socket

payload = b"mini-muduo-half-close\n" * 800

with socket.create_connection(("127.0.0.1", 2007)) as sock:
    sock.sendall(payload)

    # 客户端不再发送，但仍继续接收服务端回显。
    sock.shutdown(socket.SHUT_WR)

    received = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        received.extend(chunk)

assert bytes(received) == payload, (
    f"echo mismatch: sent={len(payload)}, received={len(received)}"
)

print(f"PASS: echoed {len(received)} bytes after half-close")