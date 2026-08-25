import struct 

HOST = ""
IP = "192.168.0.6"
PORT = 30000
MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20
MESSAGE_COUNT = 1
SESSION_ID = b"0020190130"
HEADER_FORMAT = ">10sQH"
BODY_FORMAT = ">H"
HEADER_FORMAT = ">10sQH"   # big-endian: 10-byte session, uint64 seq, uint16 count
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)   # 20 bytes
assert HEADER_SIZE == 20
 
MSG_LEN_FORMAT = ">H"      # big-endian uint16 length prefix per message
MSG_LEN_SIZE = struct.calcsize(MSG_LEN_FORMAT)  

RETRANSMIT_PORT = 30001