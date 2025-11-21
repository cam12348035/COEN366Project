import socket
from registration import registration_handler, deregistration_handler
import sys
import threading

# Server settings
HOST = '0.0.0.0'
UDP_PORT = 5000
TCP_PORT = 6000
MAX_CLIENTS = 50

peer_list = {}

# UDP socket
try:
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind((HOST, UDP_PORT))
    print('UDP Socket created')
except:
    print('Failed to create UDP. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()


# Create TCP socket
try:
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.bind(('localhost', TCP_PORT))
    tcp_sock.listen(1)
    print('TCP Socket created')
except:
    print('Failed to create TCP. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    sys.exit()


def tcp_client(client_sock, addr):
    print(f"TCP from {addr}")
    try:
        while True:
            data = client_sock.recv(4096)
            if not data:
                break
            
            message = data.decode()
            print(f"TCP received from {addr}: {message}")
            
#Gotta put in TCP requests
            response = message
            client_sock.send(response.encode())
            
    finally:
        client_sock.close()
        print(f"TCP connection closed: {addr}")

def udp_server():
    try:
        while True:
            data, addr = udp_sock.recvfrom(4096)
            message = data.decode()
            print(f"UDP received from {addr}: {message}")
            
            split_message  = message.split()
            command = split_message [0]
            
            if command == "REGISTER":
                registration_handler(split_message, addr, peer_list, MAX_CLIENTS, udp_sock)
            elif command == "DE-REGISTER":
                deregistration_handler(split_message, addr, peer_list, udp_sock)
    except KeyboardInterrupt:
        sys.exit()

def tcp_server():
    try:
        while True:
            client_sock, addr = tcp_sock.accept()
            # Each TCP peer has its own thread
            thread = threading.Thread(target=tcp_client, args=(client_sock, addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        sys.exit()



#Main code
# UDP thread
udp_thread = threading.Thread(target=udp_server)
udp_thread.daemon = True
udp_thread.start()

# TCP Thread
tcp_thread = threading.Thread(target=tcp_server)
tcp_thread.daemon = True
tcp_thread.start()
try:
    while True:
        pass
except KeyboardInterrupt:
    udp_sock.close()
    tcp_sock.close()
    print("\nServer closed")
    sys.exit()