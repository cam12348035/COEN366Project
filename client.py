import socket #for sockets
import sys
import threading


client_host = '0.0.0.0'
client_port = 5001

#UDP Socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
except socket.error:
    print('Failed to create socket')
    sys.exit()
s.bind((client_host,client_port))
host = 'localhost';
port = 5000;    
s.settimeout(3)

#asks to create register new peer
#new peer registered, added to a dict of clients, with:
#   new udp port
#   new tcp port
#   new thread

def peer_thread(name, peer_udp_port, peer_tcp_port):
    while True:
        pass

def udp_socket_creator(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error:
        print('Failed to create socket')
        sys.exit()
    s.bind((client_host,port))
    s.settimeout(3)

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

current_rq_no = 0
current_udp_port = 5001
current_tcp_port = 6001

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
                subthread = threading.Thread(target=peer_thread, args =(name, current_udp_port, current_udp_port))    
                subthread.daemon = True
                subthread.start()
                
                current_udp_port +=1
                current_udp_port +=1
                
            
            
            
                    
        elif option == "2":
            name = input("What is the name for deregistration?\n")
            msg = "DE-REGISTER " + str(current_rq_no) + " " + name



        current_rq_no += 1
        print(msg)
 
    except KeyboardInterrupt:
        print("Test closed")
        sys.exit()
        