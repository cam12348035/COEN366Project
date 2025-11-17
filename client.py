import socket
import sys
import time


# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('localhost', 10010))

# Connect the socket to the port where the server is listening
server_address = ('localhost', 10000)
print('connecting to {} port {}'.format(*server_address), file=sys.stderr)

sock.connect(server_address)

start_time = time.time()
duration = 5 * 60

try:
    
    # Send data
    while time.time() - start_time < duration:
        t0 = time.time()

        data = "message"
        message = 'PING' + str(len(data.encode())) + data
        print('sending "{}"'.format(message), file=sys.stderr)

        sock.sendall(message.encode())   # MUST encode to bytes in Python 3

        # Look for the response
        amount_received = 0
        amount_expected = len(message)

        while amount_received < amount_expected:
            data = sock.recv(16)
            amount_received += len(data)
        t1 = time.time()
        timestamp = time.asctime(time.gmtime(t0))
        print(timestamp)
        print('received "{}"'.format(data.decode()), file=sys.stderr)
        print('Timestamp of send:' + str(timestamp) + ' Diff:' + str(t1-t0))

finally:
    print('closing socket', file=sys.stderr)
    sock.close()
