'''
The configuration file for the ChordSystem
'''

import socket
import Queue
import threading
import random
import time
import sys
import pickle

socket.setdefaulttimeout(1)

MAX_LISTEN = 2
NUM_IDENTIFIER_BITS = 8
MAX_NODES = 2**NUM_IDENTIFIER_BITS

# can be later changed to include unique random ids
node_ids = range(MAX_NODES) # number of nodes could be inferred from node_ids list

node_address = {}
for i in node_ids:
    node_address[i] = ('localhost', 1234 + i)

coordinator_id = MAX_NODES
node_address[coordinator_id] = ('localhost', 1234 + coordinator_id)

time_out = 12


# Just an extra index for testing
test_id = MAX_NODES + 1
node_address[test_id] = ('localhost', 1234 + test_id)

# flag for logging messages - very crude man. Change it if you can
log_messages = False
# a commands_file has to be added
