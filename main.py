import socket

# Server settings
HOST = '0.0.0.0'
UDP_PORT = 5000
TCP_PORT = 6000
MAX_CLIENTS = 50

peer_list = {}

# UDP socket
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, UDP_PORT))
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


def registration_handler(split_message, addr):
    rq_num = split_message[1]
    name = split_message[2]
    role = split_message[3]
    ip_add = split_message[4]
    udp_port = split_message[5]
    tcp_port = split_message[6]
    storage = split_message[7]
    
    if name in peer_list:
        response = f"REGISTER-DENIED {rq_num} Name already in use"
        sock.sendto(response.encode(), addr)
        print(f"Registration denied {addr}, {name}: {response}")
        return
    
    if len(peer_list) >= MAX_CLIENTS:
        response = f"REGISTER-DENIED {rq_num} Server cannot handle additional clients"
        sock.sendto(response.encode(), addr)
        print(f"Registration denied {addr}, {name}: {response}")
        return
    
    #Register peer
    peer_list[name] = (role, ip_add, udp_port, tcp_port, storage)
    print(f"Registered: {name}, {role}")
    
    response = f"REGISTERED {rq_num}"
    sock.sendto(response.encode(), addr)
    print(f"Response sent to {addr}: {response}")

def deregistration_handler(split_message, addr):
    rq_num = split_message[1]
    name = split_message[2]
    
    if name in peer_list:
        del peer_list[name]
        print(f"De-registered: {name}")
        response = f"DE-REGISTERED {rq_num}"
        sock.sendto(response.encode(), addr)
        print(f"Sent to {addr}: {response}")
    else:
        print(f"De-register ignored: {name} not found")



#Main loop
try:
    while True:
        data, addr = sock.recvfrom(4096)
        message = data.decode()
        print(f"Received from {addr}: {message}")
        
        split_message = message.split()
        command = split_message[0]
        
        if command == "REGISTER":
            registration_handler(split_message, addr)
        elif command == "DE-REGISTER":
            deregistration_handler(split_message, addr)

except KeyboardInterrupt:
    print("Server closed")