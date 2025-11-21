
def registration_handler(split_message, addr, peer_list, max_clients, udp_sock):
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
    
    if len(peer_list) >= max_clients:
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

def deregistration_handler(split_message, addr, peer_list, udp_sock):
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
