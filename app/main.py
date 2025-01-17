import socket
import struct  # noqa: F401

#core message sizes:
# 4 bytes for the length of the message
# Example structure:
# 00 00 00 23  // message_size:        35 
# 00 12        // request_api_key:     18 INT16
# 00 04        // request_api_version: 4 INT16
# 6f 7f c6 61  // correlation_id:      1870644833 INT32
# ...

# Correct shape
# echo -n "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C
# Incorrect shape
# echo -n "000000230012674a4f74d28b00096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C

def create_message(correlation_id: int, error_code: int | None = None) -> bytes:
    message = correlation_id.to_bytes(4, byteorder="big", signed=True)
    if error_code is not None:
        message += error_code.to_bytes(2, byteorder="big", signed=True)
    message_len = len(message).to_bytes(4, byteorder="big", signed=False)
    return message_len + message

def parse_request(request: bytes) -> dict[str, int | str]:
    buff_size = struct.calcsize(">ihhi") # here the > is for big endian, i is for integer, h is for short integer, and the last i is for integer
    length, api_key, api_version, correlation_id = struct.unpack(
        ">ihhi", request[0:buff_size]
    )
    return {
        "length": length,
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id,
    }

def handle_client(client):
    request = client.recv(1024) # reading the first 1024 bytes sent by the client, this is a placeholder value for now
    print(f"[+] Recieved: {request}")
    parsed_request = parse_request(request)
    print(f"[+] Received correlation_id: {parsed_request['correlation_id']}")
    
    if 0 <= parsed_request["api_version"] <= 4:
        message = create_message(parsed_request["correlation_id"])
    else:
        message = create_message(
            parsed_request["correlation_id"], 35
        )
    client.sendall(message)
    client.close()


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addrs = server.accept()
        handle_client(client)

if __name__ == "__main__":
    main()
