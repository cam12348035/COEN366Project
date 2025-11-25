import socket #for sockets
import sys
import threading


def udp_socket_creator(port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error:
        print('Failed to create socket')
        sys.exit()
    sock.bind(('0.0.0.0',port))
    sock.settimeout(3)
    return sock
    
def send_message(msg):
    try :
        s.sendto(msg.encode(), (host, port))
        try:
            data, addr = s.recvfrom(4096)
            print('Server reply : ' + data.decode())
            return data.decode()
        except:
            print('Error timeout')
            return ""

    except socket.error as msg:
        print('Error')


#UDP Socket
client_port = 5001
s = udp_socket_creator(client_port)
host = 'localhost';
port = 5000;    




def peer_thread(name, peer_udp_port, peer_tcp_port, close_flag):
    peer_socket = udp_socket_creator(peer_udp_port)
    
    while True:
        if close_flag.is_set():
            break
        else:
            pass


current_rq_no = 0
current_udp_port = 5002
current_tcp_port = 6002
peer_dict = {}

while True:
    try:
        msg = ""
        option = input("What would you like to do: \n1: Register new peer \n2: De-register existing peer\n3: Backup a file:")
      
        if option == "1":
            name = input("What is the name for registration?\n")
            #Peer thread and information creation
            suboption = input("What role would you like: \n1: STORAGE \n2: OWNER\n3: BOTH")
            if suboption == "1":
                role = "STORAGE"
            elif suboption == "2":
                role = "OWNER"
            elif subtion == "3":
                role = "BOTH"
            
            msg = "REGISTER " + str(current_rq_no) + " " + name + " " + role + " 192.168.1.10 " + str(current_udp_port) + " " + str(current_udp_port) + " 1024MB"
            if len(send_message(msg)) != 0:
                print(f"Successful registration for {name}, starting thread")
                close_flag = threading.Event()
                subthread = threading.Thread(target=peer_thread, args =(name, current_udp_port, current_udp_port, close_flag))    
                subthread.daemon = True
                subthread.start()
                
                current_udp_port +=1
                current_udp_port +=1
                peer_dict.update({name:[subthread,close_flag]})
                     
                    
        elif option == "2":
            name = input("What is the name for deregistration?\n")
            msg = "DE-REGISTER " + str(current_rq_no) + " " + name
            if len(send_message(msg)) != 0:
                peer_dict[name][1].set()
                peer_dict.pop(name)
                print(f"Successfully de-registered {name}, closed thread")
                
                
        elif option == "3":
            name = input("From which peer would you like to backup a file?")
            if name in peer_dict:
                pass
            
            
        else:
            print("Incorrect input, please try again")


        current_rq_no += 1
        print(msg)
 
    except KeyboardInterrupt:
        print("Test closed")
        sys.exit()
        