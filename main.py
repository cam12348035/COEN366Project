#TODO:
#Install mutex/lock on at least udp_request_list and request_number_list

import time
import socket
import sys
import threading
import math


# Server settings
HOST = '0.0.0.0'
UDP_PORT = 5000
TCP_PORT = 6000
MAX_CLIENTS = 50

peer_list = {}
storage_list = {}
stored_data_mapping = {}

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

def convert_to_bytes(size_str):
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4
    }
    
    number = int(''.join(filter(str.isdigit, size_str)))
    unit = ''.join(filter(str.isalpha, size_str))
    return number * units.get(unit, 1)

def send_message(msg, port):
    try :
        udp_sock.sendto(msg.encode(), ('localhost', port))
        print(f"Sending {msg} to {port}")

    except socket.error as msg:
        print('Error')


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
    storage_int = convert_to_bytes(storage)
    peer_list[name] = [role, ip_add, udp_port, tcp_port, storage_int]
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


def backup_handler(split_message, addr, udp_sock, tcp_sock):
    rq_num = split_message[1]
    file_name = split_message[2]
    file_size = int(split_message[3])
    no_chunks = math.ceil(int(file_size)/4096)
    
    peers_for_storage = []
    chunks_per_peer = []
    try:
        for name,peer_info in peer_list.items():
            if no_chunks <= 0:
                break
            if peer_info[0] != "OWNER":
                storage_chunks = math.floor(int(peer_info[4])/4096)
                if storage_chunks > 0:
                
                    if  no_chunks - storage_chunks< 0:
                        storage_chunks = no_chunks
                        no_chunks = 0
                    else: 
                        no_chunks = no_chunks - storage_chunks
                    peers_for_storage.append(name)
                    chunks_per_peer.append(storage_chunks)
        
        
        #Remove used storage from peer
        for i in range(len(peers_for_storage)):
            temp_peer = peer_list[peers_for_storage[i]]
            temp_peer[4] = temp_peer[4] - chunks_per_peer[i]*4096
            peer_list[peers_for_storage[i]] = temp_peer
        

        #Find requesting peer name:
        peer_name = "no_name"
        for name, peer_info in peer_list.items():
            if int(peer_info[2]) == int(addr[1]):  # role is at index 0
                peer_name = name   
    
        #Send message to storage peers:
        print(f"Peers available for storage: {peers_for_storage}")
        for i in range(len(peers_for_storage)):
            msg = "STORAGE_TASK " + str(rq_num) + " " + file_name + " 4096 " + str(chunks_per_peer[i]) + " " + peer_name
            send_message(msg, int(peer_list[peers_for_storage[i]][2]))
            
        #Replies to the requester
        msg = "BACKUP_PLAN " + str(rq_num) + " " + file_name + " " + str(peers_for_storage) + " " + str(chunks_per_peer) + " " + "4096"
        print(msg)
        send_message(msg, addr[1])
        
        #Notifies each selected peer:
        last_chunk = 0
        for i in range(len(chunks_per_peer)):
            for j in range(chunks_per_peer[i]):
                msg = "STORE_REQ " + str(rq_num) + " " + file_name + " " + str(last_chunk + j)  + " " + peer_name
                send_message(msg, int(peer_list[peers_for_storage[i]][2]))
            last_chunk = last_chunk + chunks_per_peer[i]
        

    except:
    
#TODO: Add reason for denial
        msg = "BACKUP-DENIED " + str(rq_num) + " Reason" + {e}
        send_message(msg, addr[1])
    
    while True:
        time.sleep(0.5)
        for i in udp_request_list:
            message = i[0]
            message_split = message.split()
            if i[0] == "BACKUP_DONE" and int(i[1]) == rq_num:
                break

    
    request_number_list.remove(rq_num)



def main_thread():
    try:
        while True:
            data, addr = udp_sock.recvfrom(4096)
            message = data.decode()
            split_message = message.split()
            print(f"Received {message} from {addr}")

            if len(split_message) < 3:
                print("Wrong message size")
                continue

            if split_message[1] in request_number_list:
                udp_request_list.append([message,addr])
            else:
                request_number_list.append(split_message[1])
                if split_message[0] == "REGISTER":
                    subthread = threading.Thread(target=registration_handler, args =(split_message, addr, udp_sock))
                    subthread.daemon = True
                    subthread.start()

                elif split_message[0] == "DE-REGISTER":
                    subthread = threading.Thread(target=deregistration_handler, args = (split_message, addr, udp_sock))
                    subthread.daemon = True
                    subthread.start()

                elif split_message[0] == "BACKUP_REQ":
                    subthread = threading.Thread(target=backup_handler, args = (split_message, addr, udp_sock, tcp_sock))
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