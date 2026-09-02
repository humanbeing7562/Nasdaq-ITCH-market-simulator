import struct 
import numpy as np
HOST = ""
IP = "192.168.0.7"
PORT = 30000
MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20
MESSAGE_COUNT = 1
SESSION_ID = b"0020190130"
HEADER_FORMAT = ">10sQH"
BODY_FORMAT = ">H"
HEADER_FORMAT = ">10sQH"   # big-endian: 10-byte session, uint64 seq, uint16 count
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)   # 20 bytes
MAX_CONSUMERS = 4 # maximum gating consumers
assert HEADER_SIZE == 20
 
MSG_LEN_FORMAT = ">H"      # big-endian uint16 length prefix per message
MSG_LEN_SIZE = struct.calcsize(MSG_LEN_FORMAT)  

RETRANSMIT_PORT = 30001

from enum import IntEnum

class Action(IntEnum):
    ADD = 1
    CANCEL = 2
    DELETE = 3
    EXECUTE = 4
    R_CANCEL = 5
    R_ADD = 6
    
class Side(IntEnum):
    BID = 0
    ASK = 1


MAX_INSTRUMENTS = 65536       
MBP_DEPTH = 10               
SNAPSHOT_INTERVAL = 0.200     
SNAPSHOT_SHM_NAME = "mbp_snapshots"
 
SNAPSHOT_DTYPE = np.dtype([
    ('bid_price', np.int64, (MBP_DEPTH,)),
    ('bid_qty',   np.int32, (MBP_DEPTH,)),
    ('ask_price', np.int64, (MBP_DEPTH,)),
    ('ask_qty',   np.int32, (MBP_DEPTH,)),
    ('timestamp', np.int64),
    ('seqlock',   np.uint64),
])