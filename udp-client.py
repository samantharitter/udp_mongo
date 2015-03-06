import bson
import pymongo
import socket
import struct
import random

from bson.objectid import ObjectId
from bson.son import SON
from bson.py3compat import b, StringIO

## ripped shamelessly from pymongo
_INSERT = 0

_OP_MAP = {
    _INSERT: b('\x04documents\x00\x00\x00\x00\x00')
}

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648

_ZERO_32 = b('\x00\x00\x00\x00')

COLL_NAME = "test.udp"

def __pack_message(operation, data):
    """Takes message data and adds a message header based on the operation.
    Returns the resultant message string.
    """
    request_id = random.randint(MIN_INT32, MAX_INT32)
    message = struct.pack("<i", 16 + len(data))
    message += struct.pack("<i", request_id)
    message += _ZERO_32  # responseTo
    message += struct.pack("<i", operation)
    return (request_id, message + data)

def _insert_message(insert_message):
        """Build the insert message with header and GLE.
        """
        request_id, final_message = __pack_message(2002, insert_message)
        return request_id, final_message

def format_wp_insert(doc):
    data = StringIO()
    data.write(struct.pack("<i", int(False)))
    data.write(bson._make_c_string(COLL_NAME))
    message_length = begin_loc = data.tell()

    encoded = bson.BSON.encode(doc, False, bson.binary.UUID_SUBTYPE)
    encoded_length = len(encoded)
    if encoded_length > 65507:
        return None
    data.write(encoded)

    return data

def doc_from_message(message):
    doc = {}
    doc['_id'] = ObjectId()
    doc['message'] = message
    return doc

def send_over_udp(data):
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    addr = ("localhost", 26000)
    sock.sendto(data, addr)

def send(message, port):
    doc = doc_from_message(message)
    wp = format_wp_insert(doc)
    if not wp:
        print "boo there was a problem"
        return
    send_over_udp(wp.getvalue())

send("hello python", 26000)
