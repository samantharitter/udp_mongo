import bson
import pymongo
import socket
import string
import struct
import random
import time

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

    request_id, final_message = _insert_message(data.getvalue())
    return final_message

def doc_from_message(message):
    doc = {}
    doc['_id'] = ObjectId()
    doc['message'] = message
    return doc

def send_over_udp_socket(message, sock, addr):
    doc = doc_from_message(message)
    data = format_wp_insert(doc)
    if not data:
        print "boo there was a problem"
        return
    sock.sendto(data, addr)

def send_over_udp(message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    addr = ("localhost", 26000)
    send_over_udp_socket(message, sock, addr)
    sock.close()

def send_over_tcp_socket(message, sock):
    doc = doc_from_message(message)
    data = format_wp_insert(doc)
    if not data:
        print "boo there was a problem"
        return
    sock.send(data)

def send_over_tcp(message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", 27017))
    send_over_tcp_socket(message, sock)
    sock.close()

def send(message):
    send_over_tcp(message)

def random_string(n):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(n))


def time_trial_tcp_separate_connections(n):
    for x in range(0, n):
        msg = random_string(24)
        send_over_tcp(msg)
    return

def time_trial_tcp_one_connection(n):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", 27017))

    for x in range(0, n):
        msg = random_string(24)
        send_over_tcp_socket(msg, sock)

    sock.close()
    return

def time_trial_udp_one_socket(n):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    addr = ("localhost", 26000)

    for x in range(0, n):
        msg = random_string(24)
        send_over_udp_socket(msg, sock, addr)

    sock.close

def time_trial_udp_separate_sockets(n):
    for x in range(0, n):
        msg = random_string(24)
        send_over_udp(msg)
    return

def run_time_trial(n):
    ## how long does it take each implementation to send n documents?
    print "\none connection/socket:\n"

    start = time.clock()
    time_trial_tcp_one_connection(n)
    print "\tTCP:\t", time.clock() - start, " seconds"

    start = time.clock()
    time_trial_udp_one_socket(n)
    print "\tUDP:\t", time.clock() - start, " seconds"

    print "\nseparate connections/sockets:\n"

    start = time.clock()
    time_trial_tcp_separate_connections(n)
    print "\tTCP:\t", time.clock() - start, " seconds"

    start = time.clock()
    time_trial_udp_separate_sockets(n)
    print "\tUDP:\t", time.clock() - start, " seconds\n"

print "========== 100 inserts =========="
run_time_trial(100)
print "========== 1000 inserts =========="
run_time_trial(1000)
print "========== 10000 inserts =========="
run_time_trial(10000)
print "========== 100000 inserts =========="
run_time_trial(100000)
