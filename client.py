import socket #for sockets
import sys


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


current_rq_no = 0

while(1) :
#    msg = input('Enter message to send : ')            
#    split_message = message.split()
    msg = ""
    option = input("What would you like to do: \n1: Register new peer \n2: De-register existing peer\n")
  
    if option == "1":
        name = input("What is the name for registration?\n")
        msg = "REGISTER " + str(current_rq_no) + " " + name + " " + "BOTH 192.168.1.10 5001 6001 1024MB"
        
#could change to not standard later
        
    elif option == "2":
        name = input("What is the name for deregistration?\n")
        msg = "DE-REGISTER " + str(current_rq_no) + " " + name



    current_rq_no += 1
    print(msg)

    try :
        s.sendto(msg.encode(), (host, port))
        try:
            data, addr = s.recvfrom(4096)
            print('Server reply : ' + data.decode())
        except:
            print('Error timeout')

    except socket.error as msg:
        print('Error')
        
    except KeyboardInterrupt:
        print("Test closed")
        sys.exit()
        