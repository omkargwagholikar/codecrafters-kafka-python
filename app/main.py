import socket  # noqa: F401

def create_message(id):
    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client):
    request = client.recv(1024) # reading the first 1024 bytes sent by the client, this is a placeholder value for now
    print(f"[+] Recieved: {request}")
    
    correlation_id = int.from_bytes(request[8:12], byteorder='big')
    print(f"[+] Received correlation_id: {correlation_id}")
    
    client.sendall(create_message(correlation_id))
    client.close()

#core message sizes:
# 4 bytes for the length of the message
# Example structure:
# 00 00 00 23  // message_size:        35 
# 00 12        // request_api_key:     18 INT16
# 00 04        // request_api_version: 4 INT16
# 6f 7f c6 61  // correlation_id:      1870644833 INT32
# ...
# echo -n "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C

def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addrs = server.accept()
        handle_client(client)

if __name__ == "__main__":
    main()
