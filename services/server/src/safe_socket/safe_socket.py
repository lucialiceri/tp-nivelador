import socket

# TODO: Complete with a short-read/short-write tolerant implementation

RETRIES_LIMIT = 100

# keeps receiving data until matches the expected amount
def recv_all(socket: socket.socket, size):
    total_rec = socket.recv(size)

    while size > len(total_rec):
        n = socket.recv(size - len(total_rec))
        if not n:
            raise ConnectionError("Socket connection broken")
        total_rec += n
    return total_rec

# keeps sending data until matches the expected amout
def send_all(socket: socket.socket, bytes):
    total_sent = 0
    retries = 0

    while total_sent < len(bytes):
        n = socket.send(bytes[total_sent:])

        if n == 0:
            retries += 1
            if retries > RETRIES_LIMIT:
                raise ConnectionError("Socket connection broken")
            continue

        total_sent += n

    return total_sent
