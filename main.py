#TODO:
#Install mutex/lock on at least udp_request_list and request_number_list


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
udp_request_list = []
request_number_list = []


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

def registration_handler(split_message, addr, udp_sock):
    if len(split_message) < 8:
        print("Message incorrect format")
        return
    rq_num = split_message[1]
    name = split_message[2]
    role = split_message[3]
    ip_add = split_message[4]
    udp_port = split_message[5]
    tcp_port = split_message[6]
    storage = split_message[7]
    
    if name in peer_list:
        response = f"REGISTER-DENIED {rq_num} Name already in use"
        udp_sock.sendto(response.encode(), addr)
        print(f"Registration denied {addr}, {name}: {response}")
        return
    
    if len(peer_list) >= MAX_CLIENTS:
        response = f"REGISTER-DENIED {rq_num} Server cannot handle additional clients"
        udp_sock.sendto(response.encode(), addr)
        print(f"Registration denied {addr}, {name}: {response}")
        return
    
    #Register peer
    peer_list[name] = (role, ip_add, udp_port, tcp_port, storage)
    print(f"Registered: {name}, {role}")
    
    response = f"REGISTERED {rq_num}"
    udp_sock.sendto(response.encode(), addr)
    print(f"Response sent to {addr}: {response}")
    
    request_number_list.remove(rq_num)

def deregistration_handler(split_message, addr, udp_sock):
    rq_num = split_message[1]
    name = split_message[2]
    
    if name in peer_list:
        del peer_list[name]
        print(f"De-registered: {name}")
        response = f"DE-REGISTERED {rq_num}"
        udp_sock.sendto(response.encode(), addr)
        print(f"Sent to {addr}: {response}")
    else:
        print(f"De-register ignored: {name} not found")
    request_number_list.remove(rq_num)




def main_thread():
    try:
        while True:
            data, addr = udp_sock.recvfrom(4096)
            message = data.decode()
            split_message = message.split()
            if len(split_message) < 3:
                print("Wrong message size")
                continue

            if split_message[1] in request_number_list:
                udp_request_list.append([message,addr])
            else:
                request_number_list.append(split_message[1])
                if split_message[0] == "REGISTER":
                    subthread = threading.Thread(target=registration_handler, args =(split_message, addr, udp_sock))

                elif split_message[0] == "DE-REGISTER":
                    subthread = threading.Thread(target=deregistration_handler, args = (split_message, addr, udp_sock))
                    
                subthread.daemon = True
                subthread.start()
    except KeyboardInterrupt:
        sys.exit()



            



#Main code
# Main thread 
main_th = threading.Thread(target=main_thread)
main_th.daemon = True
main_th.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    udp_sock.close()
    tcp_sock.close()
    print("\nServer closed")
    sys.exit()