import socket #for sockets
import sys
import threading
import os
import binascii
import ast
import math

peer_dict = {}
backup_requests = {}


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

#UDP Socket
client_port = 5001
s = udp_socket_creator(client_port)
host = 'localhost';
server_port = 5000; 

def send_file_chunks(file_data, file_name, rq_num, storage_peers, chunks_per_peer, udp_socket):
    chunk_size=4096
    total_chunks = math.ceil(len(file_data) / chunk_size)
    error_count = 0
    last_chunk = 0
    for index, peer_name in enumerate(storage_peers):
        peer_tcp_port = int(peer_dict[peer_name][3])  
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
                udp_socket.settimeout(0.5)
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

def receive_file_chunks(peer_tcp_port, peer_name, peer_socket, sender_name, no_chunks):
    chunk_list = []
    file_name = ""
    sender_udp_port = peer_dict[sender_name][4]
    chunks_accepted = 0
    try:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.bind(('0.0.0.0', peer_tcp_port))
        tcp_sock.listen(5)
        print(f"{peer_name} TCP listener started on port {peer_tcp_port}")
        while True:
            conn, addr = tcp_sock.accept()
            print(f"{peer_name} accepted connection from {addr}")
            try:
                while int(no_chunks) > chunks_accepted:
                    header = b""
                    while not header.endswith(b'\n'):
                        header += conn.recv(1)
                    header = header.decode().strip()
                    if not header:
                        break
                    print(f"{peer_name} Received header: {header}")
                    parts = header.split()

                    if parts[0] == "SEND_CHUNK":
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
                            chunk_list.append([chunk_id, chunk_data])
                            
                            msg = f"CHUNK_OK {rq_num} {file_name} {chunk_id}"
                            send_message_no_reply(msg, peer_socket, sender_udp_port, sender_name)
                            
                            msg = f"STORE_ACK {rq_num} {file_name} {chunk_id}"
                            send_message_no_reply(msg, peer_socket, server_port, "Server")

                        else:
                            msg = f"CHUNK_ERROR {rq_num} {file_name} {chunk_id} Checksum_Mismatch"
                            send_message_no_reply(msg, peer_socket, sender_udp_port, sender_name)

                        chunks_accepted +=1
            except Exception as e:
                print(f"Error receiving chunk: {e}")
            finally:
                conn.close()
    except Exception as e:
        print(f"TCP listener error: {e}")

    return file_name, chunk_list



   




def peer_thread(name, peer_udp_port, peer_tcp_port, close_flag, backup_flag, role):
    peer_socket = udp_socket_creator(peer_udp_port)
    peer_socket.settimeout(0.001)
    backing_up = False
    file_name = ""
    file_size = 0
    file_crc32 = 0
    file_data = 0
    last_storage_name = ""
    last_storage_chunks = 0
    file_list = []
    chunk_storage_list = []
    
    while True:
        if close_flag.is_set():
            break
        if role != "STORAGE" and backup_flag.is_set():
            info = backup_requests[name] #[file_name, file_size, file_crc32, file_data, current_rq_no]
            file_name = info[0]      
            file_size = info[1]      
            file_crc32 = info[2]     
            file_data = info[3]      
            current_rq_no = info[4]  

            msg = f"BACKUP_REQ {current_rq_no} {file_name } {file_size} {file_crc32}"
            response = send_message(msg, peer_socket, server_port, "Server")
            backup_flag.clear()
            backing_up = True
        

        try:
            data, addr = peer_socket.recvfrom(4096)
            print("Got:", data.decode())
            message = data.decode()
            split_message = message.split()

            if backing_up and split_message[0] == "BACKUP_PLAN":
            
                #Getting arrays from string
                first_open = message.find('[')
                first_close = message.find(']') + 1
                second_open = message.find('[', first_close)
                second_close = message.find(']', second_open) + 1
                peer_str = message[first_open:first_close]
                chunks_str = message[second_open:second_close]
                storage_peer_list = ast.literal_eval(peer_str)
                storage_chunks_list = ast.literal_eval(chunks_str)
                
                
                send_file_chunks(file_data, file_name, int(split_message[1]), storage_peer_list, storage_chunks_list, peer_socket)
                
            if role != "OWNER" and split_message[0] == "STORAGE_TASK":
                last_storage_name = split_message[5]
                last_storage_chunks = split_message[4]

            if role != "OWNER" and split_message[0] == "STORE_REQ":
                file, chunk_list = receive_file_chunks(peer_tcp_port, name, peer_socket, last_storage_name, last_storage_chunks)
                file_list.append(file)
                chunk_storage_list.append(chunk_list)

            
        except socket.timeout:
            pass


current_rq_no = 0
current_udp_port = 5002
current_tcp_port = 6002

peer_test_list = [["Owner", "OWNER"], ["Store1", "STORAGE"], ["Store2", "STORAGE"], ["Store3", "STORAGE"]]

for peer in peer_test_list:
    name = peer[0]
    role = peer[1]
    msg = "REGISTER " + str(current_rq_no) + " " + name + " " + role + " 192.168.1.10 " + str(current_udp_port) + " " + str(current_tcp_port) + " 20KB"
    if len(send_message(msg,s, server_port, "Server")) != 0:
        print(f"Successful registration for {name}, starting thread")
        close_flag = threading.Event()
        backup_flag = threading.Event()
        subthread = threading.Thread(target=peer_thread, args =(name, current_udp_port, current_tcp_port, close_flag, backup_flag, role))    
        subthread.daemon = True
        subthread.start()

        peer_dict.update({name:[role,close_flag,backup_flag, current_tcp_port, current_udp_port]})
        current_udp_port +=1
        current_tcp_port +=1

        current_rq_no += 1


while True:
    try:
        msg = ""
        option = input("What would you like to do: \n1: Register new peer \n2: De-register existing peer\n3: Backup a file\n")
      
        if option == "1":
            
            name = input("What is the name for registration?\n")
            #Peer thread and information creation
            suboption = input("What role would you like: \n1: STORAGE \n2: OWNER\n3: BOTH")
            if suboption == "1":
                role = "STORAGE"
            elif suboption == "2":
                role = "OWNER"
            elif suboption == "3":
                role = "BOTH"
                     
            msg = "REGISTER " + str(current_rq_no) + " " + name + " " + role + " 192.168.1.10 " + str(current_udp_port) + " " + str(current_tcp_port) + " 1024MB"
            if len(send_message(msg,s, server_port, "Server")) != 0:
                print(f"Successful registration for {name}, starting thread")
                close_flag = threading.Event()
                backup_flag = threading.Event()
                subthread = threading.Thread(target=peer_thread, args =(name, current_udp_port, current_tcp_port, close_flag, backup_flag, role))    
                subthread.daemon = True
                subthread.start()
                
                peer_dict.update({name:[role,close_flag,backup_flag, current_tcp_port, current_udp_port]})
                current_udp_port +=1
                current_tcp_port +=1
                current_rq_no += 1

                    
        elif option == "2":
            name = input("What is the name for deregistration?\n")
            msg = "DE-REGISTER " + str(current_rq_no) + " " + name
            if len(send_message(msg,s, server_port, "Server")) != 0:
                peer_dict[name][1].set()
                peer_dict.pop(name)
                print(f"Successfully de-registered {name}, closed thread")
            current_rq_no += 1

                
        elif option == "3":
            name = input("From which peer would you like to backup a file?")
            if name in peer_dict:
                if peer_dict[name][0] == "STORAGE":
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
                        peer_dict[name][2].set()
            current_rq_no += 1

                        


                    
                
            
            
        else:
            print("Incorrect input, please try again")


        print(msg)
 
    except KeyboardInterrupt:
        print("Test closed")
        sys.exit()
        