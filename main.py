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
peer_list_lock = threading.Lock()
stored_data_mapping = {}
stored_data_lock = threading.Lock()

udp_request_list = []
udp_request_lock = threading.Lock()
request_number_list = []
request_number_lock = threading.Lock()
last_heartbeat = {}
heartbeat_lock = threading.Lock()
TIMEOUT = 15  # seconds

# UDP socket
try:
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind((HOST, UDP_PORT))
    print('UDP Socket created')
except Exception as exc:
    print(f'Failed to create UDP socket: {exc}')
    sys.exit()


# Create TCP socket
try:
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.bind(('localhost', TCP_PORT))
    tcp_sock.listen(1)
    print('TCP Socket created')
except Exception as exc:
    print(f'Failed to create TCP socket: {exc}')
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
    with peer_list_lock:
        if name in peer_list:
            response = f"REGISTER-DENIED {rq_num} Name already in use"
            udp_sock.sendto(response.encode(), addr)
            print(f"Registration denied {addr}, {name}: {response}")
            return
            
    with peer_list_lock:
        if len(peer_list) >= MAX_CLIENTS:
            response = f"REGISTER-DENIED {rq_num} Server cannot handle additional clients"
            udp_sock.sendto(response.encode(), addr)
            print(f"Registration denied {addr}, {name}: {response}")
            return
    
    #Register peer
    storage_int = convert_to_bytes(storage)
    with peer_list_lock:
        peer_list[name] = [role, ip_add, udp_port, tcp_port, storage_int]
    with heartbeat_lock:
        last_heartbeat[name] = time.time()
    print(f"Registered: {name}, {role}")
    
    response = f"REGISTERED {rq_num}"
    udp_sock.sendto(response.encode(), addr)
    print(f"Response sent to {addr}: {response}")
    with request_number_lock:
        request_number_list.remove(rq_num)

def deregistration_handler(split_message, addr, udp_sock):
    rq_num = split_message[1]
    name = split_message[2]
    
    with peer_list_lock:
        if name in peer_list:
            del peer_list[name]
            with heartbeat_lock:
                last_heartbeat.pop(name, None)
            print(f"De-registered: {name}")
            response = f"DE-REGISTERED {rq_num}"
            udp_sock.sendto(response.encode(), addr)
            print(f"Sent to {addr}: {response}")
        else:
            print(f"De-register ignored: {name} not found")
    with request_number_lock:
        request_number_list.remove(rq_num)


def backup_handler(split_message, addr, udp_sock, tcp_sock):
    rq_num    = split_message[1]
    peer_name = split_message[2]
    file_name = split_message[3]
    file_size = int(split_message[4])
    no_chunks = math.ceil(file_size / 4096)
    
    peers_for_storage = []
    chunks_per_peer = []
    peers_id_for_storage = []
    with peer_list_lock:
        if peer_name not in peer_list:
            print("Unknown backup requester:", peer_name)
            msg = f"BACKUP-DENIED {rq_num} {file_name} Unknown_peer"
            send_message(msg, addr[1])
            return
    
    try:
        with peer_list_lock:
            temp_peer_list = peer_list
        for name,peer_info in temp_peer_list.items():
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
                    peers_id_for_storage.append(peer_info[3])
                    
        
        
        #Remove used storage from peer
        with peer_list_lock:
            for i in range(len(peers_for_storage)):
                temp_peer = peer_list[peers_for_storage[i]]
                temp_peer[4] = temp_peer[4] - chunks_per_peer[i]*4096
                peer_list[peers_for_storage[i]] = temp_peer
          
    
        #Send message to storage peers:
        print(f"Peers available for storage: {peers_for_storage}")
        for i in range(len(peers_for_storage)):
            msg = "STORAGE_TASK " + str(rq_num) + " " + file_name + " 4096 " + str(chunks_per_peer[i]) + " " + peer_name + " " + str(addr[1])
            with peer_list_lock:
                send_message(msg, int(peer_list[peers_for_storage[i]][2]))
            
        #Replies to the requester
        msg = "BACKUP_PLAN " + str(rq_num) + " " + file_name + " " + str(peers_for_storage) + " " + str(chunks_per_peer) + " " + str(peers_id_for_storage) + " 4096"
        print(msg)
        send_message(msg, addr[1])
        
        #Notifies each selected peer AND record mapping of chunks
        key = (peer_name, file_name)
        with stored_data_lock:
            if key not in stored_data_mapping:
                stored_data_mapping[key] = []
        last_chunk = 0
        for i in range(len(chunks_per_peer)):
            storage_peer = peers_for_storage[i]
            for j in range(chunks_per_peer[i]):
                chunk_id = last_chunk + j
                peer_tcp = peers_id_for_storage[i]
                msg = "STORE_REQ " + str(rq_num) + " " + file_name + " " + str(chunk_id) + " " + peer_name
                with peer_list_lock:
                    send_message(msg, int(peer_list[storage_peer][2]))
                with stored_data_lock:
                    stored_data_mapping[key].append({
                        "peer": storage_peer,
                        "tcp_id": peer_tcp,
                        "chunk_id": chunk_id
                        })
            last_chunk = last_chunk + chunks_per_peer[i]
        

    except:
    
#TODO: Add reason for denial
        msg = "BACKUP-DENIED " + str(rq_num) + " Reason" + {Exception}
        send_message(msg, addr[1])
    
    while True:
        time.sleep(0.5)
        temp_req = ""
        with udp_request_lock:
            temp_req = udp_request_list
        for i in temp_req:
            message = i[0]
            message_split = message.split()
            if i[0] == "BACKUP_DONE" and int(i[1]) == rq_num:
                break

    with request_number_lock:
        request_number_list.remove(rq_num)

def heartbeat_watcher():
  while True:
      now = time.time()
      with peer_list_lock:
        temp_peer_list = peer_list
      for peer in list(temp_peer_list.keys()):
        with heartbeat_lock:
            last_seen = last_heartbeat.get(peer)
        if last_seen is None or now - last_seen > TIMEOUT:
            mark_as_dead(peer)
      time.sleep(2)

def mark_as_dead(peer):
    print(f"Peer {peer} has timed out. Marking as dead.")
    with peer_list_lock:
        if peer in peer_list:
            del peer_list[peer]
    with heartbeat_lock:
        if peer in last_heartbeat:
            del last_heartbeat[peer]

def restore_req_handler(split_message, addr, udp_sock):
    # RESTORE_REQ RQ# File_Name
    rq_num = split_message[1]
    file_name = split_message[2]

    # Find owner peer name from UDP port (same trick as in backup_handler)
    owner_name = None
    with peer_list_lock:
        for name, peer_info in peer_list.items():
            if int(peer_info[2]) == int(addr[1]):  # peer_info[2] is udp_port
                owner_name = name
                break

    if owner_name is None:
        print("RESTORE_REQ from unknown peer at", addr)
        response = f"RESTORE_FAIL {rq_num} {file_name} Unknown_peer"
        udp_sock.sendto(response.encode(), addr)
        return

    key = (owner_name, file_name)
    temp_data = ""
    with stored_data_lock:
        temp_data = stored_data_mapping
    if key not in temp_data:
        print("No backup info for", key)
        response = f"RESTORE_FAIL {rq_num} {file_name} File_not_found"
        udp_sock.sendto(response.encode(), addr)
        return
        
    with stored_data_lock:
        entries = stored_data_mapping[key]   # list of {"peer": ..., "chunk_id": ...}

    # Build mapping: peer -> list of chunk_ids
    mapping = {}
    for entry in entries:
        p = entry["peer"]
        tcp_id = entry["tcp_id"]
        cid = entry["chunk_id"]
        key = f"{p}:{tcp_id}"
        mapping.setdefault(key, []).append(cid)

    # Encode mapping as: [Store1:0,1;Store2:2,3]
    parts = []
    for key, chunk_ids in mapping.items():
        chunk_str = ",".join(str(c) for c in chunk_ids)
        parts.append(f"{p}:{tcp_id}:{chunk_str}")
    mapping_str = ";".join(parts)

    response = f"RESTORE_PLAN {rq_num} {file_name} [{mapping_str}]"
    udp_sock.sendto(response.encode(), addr)
    print(f"Sent to {addr}: {response}")



def main_thread():
    try:
        while True:
            data, addr = udp_sock.recvfrom(4096)
            message = data.decode()
            split_message = message.split()
            print(f"Received {message} from {addr}")

            if split_message[0] == "HEARTBEAT":
                if len(split_message) >= 3:
                    peer_name = split_message[2]
                    with heartbeat_lock:
                        last_heartbeat[peer_name] = time.time()
                    print(f"Heartbeat received from {peer_name}")
                else:
                    print("Malformed HEARTBEAT message ignored")
                continue
            
            if len(split_message) < 3:
                print("Wrong message size")
                continue
            with request_number_lock:
                temp_request_list  = request_number_list
            if split_message[1] in temp_request_list:
                with udp_request_lock:
                    udp_request_list.append([message,addr])
            else:
                with request_number_lock:
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
                elif split_message[0] == "BACKUP_REQ":
                    subthread = threading.Thread(target=backup_handler, args=(split_message, addr, udp_sock, tcp_sock))
                    subthread.daemon = True
                    subthread.start()

                elif split_message[0] == "RESTORE_REQ":
                    subthread = threading.Thread(target=restore_req_handler, args=(split_message, addr, udp_sock))
                    subthread.daemon = True
                    subthread.start()


    except KeyboardInterrupt:
        sys.exit()



            



#Main code
# Main thread 
main_th = threading.Thread(target=main_thread)
main_th.daemon = True
main_th.start()

heartbeat_th = threading.Thread(target=heartbeat_watcher)
heartbeat_th.daemon = True
heartbeat_th.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    udp_sock.close()
    tcp_sock.close()
    print("\nServer closed")
    sys.exit()