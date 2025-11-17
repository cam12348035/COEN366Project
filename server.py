import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = ('localhost', 10000)

print('starting up on {} port {}'.format(*server_address), file=sys.stderr)

sock.bind(server_address)
sock.listen(1)   # Server starts listening

while True:
    print('waiting for a connection', file=sys.stderr)
    connection, client_address = sock.accept()

    try:
        print('connection from {}'.format(client_address), file=sys.stderr)

        while True:
            data = connection.recv(16)
            print('received {!r}'.format(data), file=sys.stderr)

            if data:
                print('sending data back to the client', file=sys.stderr)
                data1 = data.decode()
                data2 = 'PONG'+str(data1[4:])
                connection.sendall(data2.encode())
            else:
                print('no more data from {}'.format(client_address), file=sys.stderr)
                break

    finally:
        connection.close()
