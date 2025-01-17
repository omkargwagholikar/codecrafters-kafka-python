import socket
import struct
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
TAG_BUFFER_size = 1  # Compact array tagging is typically 1 byte
throttle_time_ms_size = 4

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
    INVALID_REQUEST = 37

class Message:
    def __init__(self, header: bytes, body: bytes):
        # Parse the message directly from the header instead of body
        self.header = header
        self.body = body
        
        # Extract fields from header (similar to correct implementation)
        self.request_api_key = int.from_bytes(header[:2], byteorder="big")
        self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
        self.client_id = int.from_bytes(header[8:], byteorder="big") if len(header) > 8 else 0
        
        self.error_code = (
            ErrorCode.NONE
            if 0 <= self.request_api_version <= 4
            else ErrorCode.UNSUPPORTED_VERSION
        )
        if self.error_code is ErrorCode.UNSUPPORTED_VERSION:
            print(f"[-] Unsupported version: {self.request_api_version}")

    def create_message_apiversion(self) -> bytes:
        # Create response message following the correct format
        response_header = self.correlation_id.to_bytes(4, byteorder="big")
        
        response_body = (
            self.error_code.value.to_bytes(2, byteorder="big")  # error_code
            + (2).to_bytes(1, byteorder="big")  # num_api_keys (1 + 1)
            + (18).to_bytes(2, byteorder="big")  # api_key
            + (0).to_bytes(2, byteorder="big")   # min_version
            + (4).to_bytes(2, byteorder="big")   # max_version
            + (0).to_bytes(2, byteorder="big")   # TAG_BUFFER
            + (0).to_bytes(4, byteorder="big")   # throttle_time_ms
        )
        
        # Calculate total size and create final message
        total_size = len(response_header) + len(response_body)
        return total_size.to_bytes(4, byteorder="big") + response_header + response_body

def handle_client(client: socket.socket):
    try:
        data = client.recv(1024)
        if not data:
            print("[-] No data received")
            return

        print(f"[+] Received: {data.hex()}")
        
        # The first 4 bytes are the size, then header starts
        message_size = int.from_bytes(data[:4], byteorder="big")
        header = data[4:16]  # Header is 12 bytes: api_key(2) + version(2) + correlation_id(4) + client_id(4)
        body = data[16:]
        
        message = Message(header, body)
        print(f"[+] Received correlation_id: {message.correlation_id}")
        
        response = message.create_message_apiversion()
        print(f"[+] Sending response: {response.hex()}")
        client.sendall(response)

    except Exception as e:
        print(f"[-] Error: {e}")
    finally:
        # client.close()
        print("Client not closed as this lets us handle sequential requests")


def main():
    print("Server starting on localhost:9092...")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        try:
            client, address = server.accept()
            print(f"[+] Connection from {address}")
            handle_client(client)
        except KeyboardInterrupt:
            print("\n[!] Shutting down server.")
            server.close()
            break
        except Exception as e:
            print(f"[-] Unexpected error: {e}")

if __name__ == "__main__":
    main()