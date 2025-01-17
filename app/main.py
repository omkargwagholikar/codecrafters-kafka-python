import socket
import struct  # noqa: F401
from enum import Enum, unique

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

# Sizes in bytes
request_api_key_size = 2
request_api_version_size = 2
correlation_id_size = 4
error_code_size = 2
min_version_size = 2
max_version_size = 2
TAG_BUFFER_size = 2
throttle_time_ms_size = 4

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
    INVALID_REQUEST = 37

class Message:
    body: dict[str, int | str]
    header: bytes
    size: int
    error_code: ErrorCode
    tagged_fields: str
    throttle_time_ms: int

    def __init__(self, header:bytes, body:bytes):
        self.header = header
        self.size = len(header) + len(body)
        self.body = self.parse_request(body)
        self.error_code = ErrorCode.NONE if 0 <= self.body["api_version"] <= 4 else ErrorCode.UNSUPPORTED_VERSION
    
    def parse_request(self, request: bytes) -> dict[str, int | str]:
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
    
    def create_message_apiversion(self) -> bytes:
        # # ApiVersion V3 Response Body | Ref.: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
        # # error_code [api_keys] throttle_time_ms TAG_BUFFER
        # # error_code INT16# num_api_keys => empirically VARINT of N + 1 for COMPACT_ARRAY
        # # 255 / 11111111 => 126
        # # 127 / 01111111 => 126
        # #  63 / 00111111 =>  62
        # # api_key INT16
        # # min_version INT16
        # # max_version INT16
        # # TAG_BUFFER -> empirically INT16
        # # throttle_time_ms INT32
        min_version, max_version = 0, 4
        throttle_time_ms = 0
        tag_buffer = b"\x00"
        response_body = (
            self.error_code.value.to_bytes(2, byteorder="big")
            + int(2).to_bytes(1, byteorder="big")
            + self.body["api_key"].to_bytes(2, byteorder='big')
            + min_version.to_bytes(2, byteorder="big")
            + max_version.to_bytes(2, byteorder="big")
            + tag_buffer
            + throttle_time_ms.to_bytes(4, byteorder='big')
            + tag_buffer
        )
        
        response_length = len(self.header) + len(response_body)
        return int(response_length).to_bytes(4, byteorder='big') + self.header + response_body

    
def handle_client(client):
    request = client.recv(1024) # reading the first 1024 bytes sent by the client, this is a placeholder value for now
    print(f"[+] Recieved: {request}")
    
    message = Message(request[:4], request[4:])
    print(f"[+] Received correlation_id: {message.body['correlation_id']}")

    response = message.create_message_apiversion()
    
    client.sendall(response)
    client.close()


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addrs = server.accept()
        handle_client(client)

if __name__ == "__main__":
    main()
