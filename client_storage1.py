import socket #for sockets
import sys
import threading
import os
import binascii
import ast
import math
import time
import zlib

backup_requests = {}
restore_requests = {}
listener_started = {}
global no_chunks


name = "Storage1"
suboption = "1"
udp_port = 5002
tcp_port = 6002
close_flag = False
backup_flag = False
restore_flag = False

option_lock = threading.Lock()
last_option = -1

host = 'localhost';
server_port = 5000; 

def restore_file(owner_name, rq_num, file_name, mapping_str):
    """
    mapping_str: string like 'Store1:TCPID:0,1,2,3,4,5,6,7,8;Store2:...' (without brackets)
    Uses peer_dict to find TCP ports, pulls each chunk via GET_CHUNK,
    and writes restored_<file_name>.
    Returns True on success, False otherwise.
    """
    # parse mapping into dict {peer_name: [chunk_ids]}
    peer_chunks = {}
    if mapping_str.strip():
        for entry in mapping_str.split(";"):
            p,tcp_id, chunks = entry.split(":")
            chunk_ids = [int(c) for c in chunks.split(",") if c]
            peer_chunks[p] = [chunk_ids, tcp_id]

    restored_dir = "restored_files"
    os.makedirs(restored_dir, exist_ok=True)
    restored_path = os.path.join(restored_dir, f"restored_{file_name}")

    chunk_data_map = {}

    for peer_name, chunk_info in peer_chunks.items():
        for cid in chunk_info[0]:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", int(chunk_info[1])))

            header = f"GET_CHUNK {rq_num} {file_name} {cid}\n"
            sock.sendall(header.encode())

            # read CHUNK_DATA header
            resp_header = b""
            while not resp_header.endswith(b"\n"):
                part = sock.recv(1)
                if not part:
                    break
                resp_header += part
            resp_header = resp_header.decode().strip()
            parts = resp_header.split()
            if parts[0] != "CHUNK_DATA":
                print("Unexpected response:", resp_header)
                sock.close()
                return False

            _, _, _, recv_cid, checksum_str, size_str = parts
            size = int(size_str)
            expected_checksum = int(checksum_str)

            data = b""
            while len(data) < size:
                chunk = sock.recv(size - len(data))
                if not chunk:
                    break
                data += chunk
            sock.close()

            if len(data) != size:
                print("Short read for chunk", cid)
                return False

            actual_checksum = zlib.crc32(data) & 0xFFFFFFFF
            if actual_checksum != expected_checksum:
                print("Checksum mismatch for chunk", cid)
                return False

            chunk_data_map[int(recv_cid)] = data

    # write chunks in order
    with open(restored_path, "wb") as f:
        for cid in sorted(chunk_data_map.keys()):
            f.write(chunk_data_map[cid])

    print(f"[{owner_name}] Restored file written to {restored_path}")
    return True



def udp_socket_creator(port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error:
        print('Failed to create socket')
        sys.exit()
    sock.bind(('0.0.0.0',port))
    sock.settimeout(3)
    return sock
    
def send_message(msg, sock, send_port, receiver_name):
    try :
        sock.sendto(msg.encode(), (host, send_port))
        print(f"Sending {msg} to {receiver_name}")
        try:
            data, addr = sock.recvfrom(4096)
            print(f"{receiver_name} reply : {data.decode()}")
            return data.decode()
        except:
            print('Error timeout')
            return ""

    except socket.error as msg:
        print('Error')

def send_message_no_reply(msg, sock, send_port, receiver_name):
    try :
        sock.sendto(msg.encode(), (host, send_port))
        print(f"Sending {msg} to {receiver_name}")
    except socket.error as msg:
        print('Error')



def send_file_chunks(file_data, file_name, rq_num, storage_peers, chunks_per_peer, address_per_peer, udp_socket):
    chunk_size=4096
    total_chunks = math.ceil(len(file_data) / chunk_size)
    error_count = 0
    last_chunk = 0
    for index, peer_name in enumerate(storage_peers):
        peer_tcp_port = int(address_per_peer[index])  
        chunk_id = 0
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.settimeout(5)
            tcp_sock.connect(('localhost', peer_tcp_port))
            print(f"Connected to {peer_name} on TCP port {peer_tcp_port}")
            
            while chunk_id < chunks_per_peer[index]:
                start = (chunk_id+last_chunk) * chunk_size
                end = min(start + chunk_size, len(file_data))
                chunk_data = file_data[start:end]
                
                # Calculate checksum
                chunk_crc32 = binascii.crc32(chunk_data) & 0xffffffff
                
                header = f"SEND_CHUNK {rq_num} {file_name} {chunk_id+last_chunk} {len(chunk_data)} {chunk_crc32}\n"
                tcp_sock.sendall(header.encode())
                tcp_sock.sendall(chunk_data)
                
                print(f"Sent chunk {chunk_id+last_chunk} to {peer_name}")
                                
                chunk_id += 1
                
                #Wait for answer
                udp_socket.settimeout(1)
                data, addr = udp_socket.recvfrom(4096)
                print(f"{addr[1]} reply : {data.decode()}")
                message = data.decode()
                message_split = message.split()
                if error_count<=3 and not data and message_split[0] == "CHUNK_OK": 
                    chunk_id -= 1
                    error_count +=1
                else:
                    error_count = 0
                   
                            
            tcp_sock.close()
            last_chunk = last_chunk + chunks_per_peer[index]
            
            if last_chunk == total_chunks:
                msg = f"BACKUP_DONE {rq_num} {file_name}"
                send_message_no_reply(msg, udp_socket, server_port, "Server")
        except Exception as e:
            print(f"Error sending to {peer_name}: {e}")

def receive_file_chunks(peer_tcp_port, peer_name, client_socket, sender_name, no_chunks, sender_udp_port):
    """
    Long-lived TCP server on a storage peer.
    Handles:
      - SEND_CHUNK (from owners during backup)
      - GET_CHUNK (from owners during restore)
    """
    import zlib

    try:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.bind(('0.0.0.0', peer_tcp_port))
        tcp_sock.listen(5)
        print(f"{peer_name} TCP listener started on port {peer_tcp_port}")

        while True:
            conn, addr = tcp_sock.accept()
            print(f"{peer_name} accepted connection from {addr}")
            try:
                while True:
                    header = b""
                    # read header line
                    while not header.endswith(b'\n'):
                        chunk = conn.recv(1)
                        if not chunk:
                            break
                        header += chunk
                    if not header:
                        break

                    header = header.decode().strip()
                    print(f"{peer_name} Received header: {header}")
                    parts = header.split()
                    cmd = parts[0]

                    if cmd == "SEND_CHUNK":
                        # existing backup behaviour
                        rq_num = parts[1]
                        file_name = parts[2]
                        chunk_id = int(parts[3])
                        chunk_size = int(parts[4])
                        expected_checksum = int(parts[5])

                        chunk_data = b""
                        while len(chunk_data) < chunk_size:
                            chunk_data += conn.recv(chunk_size - len(chunk_data))

                        received_checksum = binascii.crc32(chunk_data) & 0xffffffff
                        if received_checksum == expected_checksum:
                            # save chunk to disk so we can serve it later
                            chunk_filename = f"storage_{peer_name}_{file_name}_chunk_{chunk_id}"
                            with open(chunk_filename, "wb") as f:
                                f.write(chunk_data)

                            msg = f"CHUNK_OK {rq_num} {file_name} {chunk_id}"
                            send_message_no_reply(msg, client_socket, sender_udp_port, sender_name)

                            msg = f"STORE_ACK {rq_num} {file_name} {chunk_id}"
                            send_message_no_reply(msg, client_socket, server_port, "Server")
                        else:
                            msg = f"CHUNK_ERROR {rq_num} {file_name} {chunk_id} Checksum_Mismatch"
                            send_message_no_reply(msg, client_socket, sender_udp_port, sender_name)

                    elif cmd == "GET_CHUNK":
                        # new: serve chunk back to owner
                        rq_num = parts[1]
                        file_name = parts[2]
                        chunk_id = int(parts[3])

                        chunk_filename = f"storage_{peer_name}_{file_name}_chunk_{chunk_id}"
                        try:
                            with open(chunk_filename, "rb") as f:
                                chunk_data = f.read()
                        except FileNotFoundError:
                            # simple fail: checksum -1, size 0
                            resp = f"CHUNK_DATA {rq_num} {file_name} {chunk_id} -1 0\n"
                            conn.sendall(resp.encode())
                            continue

                        checksum = zlib.crc32(chunk_data) & 0xFFFFFFFF
                        size = len(chunk_data)

                        resp = f"CHUNK_DATA {rq_num} {file_name} {chunk_id} {checksum} {size}\n"
                        conn.sendall(resp.encode())
                        conn.sendall(chunk_data)

                # end inner while
            except Exception as e:
                print(f"Error in TCP handler for {peer_name}: {e}")
            finally:
                conn.close()

    except Exception as e:
        print(f"TCP listener error on {peer_name}: {e}")



def heartbeat_sender(peer_name, client_socket):
    seq = 0
    while True:
        msg = f"HEARTBEAT {seq} {peer_name} {time.time()}"
        send_message_no_reply(msg, client_socket, server_port, "Server")
        seq += 1
        time.sleep(15)

def input_thread():
    global last_option
    while True:
        if last_option == -1:
            option = input(
                "What would you like to do: \n"
                "1: De-register existing peer\n"
                "2: Backup a file\n"
                "3: Restore a file\n"
            )        
            last_option = option






current_rq_no = 0

client_socket = udp_socket_creator(udp_port)
client_socket.settimeout(0.1)

#initial registration loop
#    name = input("What is the name for registration?\n")
#Peer thread and information creation
#    suboption = input("What role would you like: \n1: STORAGE \n2: OWNER\n3: BOTH")
#    udp_port = int(input("What udp port would you like?"))
#    tcp_port = int(input("What tcp port would you like?"))



memory_size = 5000000
if suboption == "1":
    role = "STORAGE"
elif suboption == "2":
    role = "OWNER"
elif suboption == "3":
    role = "BOTH"
msg = (
            "REGISTER " + str(current_rq_no) + " " + name + " " + role +
            " 192.168.1.10 " + str(udp_port) + " " +
            str(tcp_port) + " " + str(memory_size)
        )
reply = send_message(msg,client_socket, server_port, "Server")
if len(reply) != 0:
    split_reply = reply.split()
    if split_reply[0] == "REGISTERED":
        print(f"Successful registration for {name}, starting thread")
        current_rq_no += 1
    else:
        print(f"Error registering, received: {reply}")
else:
    print("Error registering")
    sys.exit()


heartbeat_thread = threading.Thread(target=heartbeat_sender, args=(name, client_socket))
heartbeat_thread.daemon = True
heartbeat_thread.start()

in_thread = threading.Thread(target=input_thread)
in_thread.daemon = True
in_thread.start()

backing_up = False
restoring = False
file_name = ""
file_size = 0
file_crc32 = 0
file_data = 0
last_storage_name = ""
last_storage_chunks = 0
last_storage_id = 0
file_list = []
chunk_storage_list = []


while True:
    try:
        msg = ""
        if last_option != -1:
            current_rq_no += 1
            option = last_option
            if option == "1":
                name = input("What is the name for deregistration?\n")
                msg = "DE-REGISTER " + str(current_rq_no) + " " + name
                reply = send_message(msg,client_socket, server_port, "Server")
                split_reply = reply.split()
                if split_reply[0] == "DE-REGISTERED":
                    print(f"Successfully de-registered {name}, closing program")
                    sys.exit()
                current_rq_no += 1
                last_option = -1

                    
            elif option == "2":
                
                if role == "STORAGE":
                    print("This peer is a storage, and cannot backup a file")
                else:
                    file_name = input("What file would you like to backup?")
                    if os.path.isfile(file_name):
                        print("File exists")
                        with open(file_name, 'rb') as f:
                                file_data = f.read()
                        file_size = os.path.getsize(file_name)
                        file_crc32 = binascii.crc32(file_data) & 0xffffffff
                        backup_requests.update({name:[file_name, file_size, file_crc32, file_data, current_rq_no]})
                        backup_flag = True
                last_option = -1
                current_rq_no += 1

                            
            elif option == "3":
                if role == "STORAGE":
                    print("This peer is a storage, and cannot request restore")
                else:
                    file_name = input("What file would you like to restore?")
                    restore_requests.update({name:[file_name, current_rq_no]})
                    restore_flag = True
                        
                current_rq_no += 1
                last_option = -1

      
            else:
                print("Incorrect input, please try again")




    
        if role != "STORAGE" and backup_flag:
            info = backup_requests[name] #[file_name, file_size, file_crc32, file_data, current_rq_no]
            file_name = info[0]      
            file_size = info[1]      
            file_crc32 = info[2]     
            file_data = info[3]      
            current_rq_no = info[4]  

            msg = f"BACKUP_REQ {current_rq_no} {name} {file_name} {file_size} {file_crc32}"
            response = send_message(msg, client_socket, server_port, "Server")
            backup_flag = False
            backing_up = True

            print(msg)
            
        if role != "STORAGE" and restore_flag:
            info = restore_requests[name]  # [file_name, current_rq_no]
            file_name = info[0]
            current_rq_no = info[1]

            # Ask the server for the restore plan
            msg = f"RESTORE_REQ {current_rq_no} {file_name}"
            response = send_message(msg, client_socket, server_port, "Server")
            restore_flag = False

            if not response:
                print(f"{name}: no response to RESTORE_REQ")
            else:
                parts = response.split()
                if parts[0] != "RESTORE_PLAN":
                    print(f"{name}: unexpected response to RESTORE_REQ:", response)
                else:
                    # RESTORE_PLAN RQ# File_Name [Store1:6002:0,1,2,3,4,5,6,7,8]
                    rq_num = int(parts[1])
                    fname = parts[2]
                    mapping_str = " ".join(parts[3:]).strip("[]")  # Store1:0,1,...

                    success = restore_file(name, rq_num, fname, mapping_str)

                    # Tell server if restore worked
                    if success:
                        msg2 = f"RESTORE_OK {rq_num} {fname}"
                    else:
                        msg2 = f"RESTORE_FAIL {rq_num} {fname} Restore_error"
                    send_message_no_reply(msg2, client_socket, server_port, "Server")
        message = ""
        try:
            data, addr = client_socket.recvfrom(4096)
            print("Got:", data.decode())
            message = data.decode()
            split_message = message.split()
        except:
            pass
        
        if len(message) != 0:
            if backing_up and split_message[0] == "BACKUP_PLAN":
            
                #Getting arrays from string
                first_open = message.find('[')
                first_close = message.find(']') + 1
                second_open = message.find('[', first_close)
                second_close = message.find(']', second_open) + 1
                third_open = message.find('[', second_close)
                third_close = message.find(']', third_open) + 1

                peer_str = message[first_open:first_close]
                chunks_str = message[second_open:second_close]
                peer_addr_str = message[third_open:third_close]

                storage_peer_list = ast.literal_eval(peer_str)
                storage_chunks_list = ast.literal_eval(chunks_str)
                storage_peer_addr_list = ast.literal_eval(peer_addr_str)
                
                
                send_file_chunks(file_data, file_name, int(split_message[1]), storage_peer_list, storage_chunks_list, storage_peer_addr_list, client_socket)
            # if restoring and split_message[0] == "RESTORE_PLAN":
            #     print(f"{name} got RESTORE_PLAN:", message)
            #     # TODO later: parse and start TCP GET_CHUNK here
            #     restoring = False

            if role != "OWNER" and split_message[0] == "STORAGE_TASK":
                last_storage_name = split_message[5]
                last_storage_chunks = int(split_message[4])
                last_storage_id = int(split_message[6])

            if role != "OWNER" and split_message[0] == "STORE_REQ":
                if not last_storage_name:
                    print("STORE_REQ received before STORAGE_TASK assignment; ignoring")
                    continue

                # Start the TCP listener only once for this storage peer
                if not listener_started.get(name, False):
                    listener_thread = threading.Thread(
                        target=receive_file_chunks,
                        args=(tcp_port, name, client_socket, last_storage_name, last_storage_chunks,last_storage_id),
                        daemon=True
                    )
                    listener_thread.start()
                    listener_started[name] = True
                    
        
    except KeyboardInterrupt:
        print("Test closed")
        sys.exit()
        