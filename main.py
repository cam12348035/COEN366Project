import socket
from registration import registration_handler, deregistration_handler

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


#Main loop
try:
    while True:
        data, addr = udp_sock.recvfrom(4096)
        message = data.decode()
        print(f"Received from {addr}: {message}")
        
        split_message = message.split()
        command = split_message[0]
        
        if command == "REGISTER":
            registration_handler(split_message, addr, peer_list, MAX_CLIENTS, udp_sock)
        elif command == "DE-REGISTER":
            deregistration_handler(split_message, addr, peer_list, udp_sock)

except KeyboardInterrupt:
    print("Server closed")