'''
Message layer of the chord system
'''

from client import *
from server import *

# Some notes on comments

# @magicconstant - some hard numbers which need to be changed in later implementations

class Message:
    """
    A simple structure to hold all the parameters of a message to be sent by a node
    The message need not be limited to a string since it would be pickled while it is sent by the client thread.
    """
    def __init__(self, node_id, msg, delivary_time):
        self.node_id = node_id
        self.msg = msg
        self.delivary_time = delivary_time

class MessagingLayer(threading.Thread):
    """Generic messaging over TCP for implementing various consistency models.

    Abstraction of the server/client representation of a node in a distributed system sending and receiving messages.
    Features -  FIFO for messages sent.
                Simulated a delay for messages sent

    @change description - Need to put it all in a message layer, so the main thread need to only read/write and wait for return/ack.

    Methods -
    @change - remove description - put it in appropriate place - _send_message - Send a message to another node in the distributed system - Format: send_message(node_id, msg_string)
    stop - Stop the server at this node from server. Need to call this before releasing the object to stop the Server threading
    """
    def __init__(self, node_id, send_message_queue, received_message_queue, message_lock):
        """
        send_message_queue        - messages to be sent from the main node queue
        received_message_queue    - any message received is put into this queue for the node layer to access it
        message_lock              - A lock to synchronise accesses to these to two data structures
        """
        super(MessagingLayer, self).__init__()
        self.node_id = node_id
        self._stoprequest = threading.Event()
        self._message_lock = message_lock
        self._send_message_queue = send_message_queue
        self._received_message_queue = received_message_queue
        socket.setdefaulttimeout(1)

        # Using list instead of Queue to later support non-FIFO processing of messages received. Hence the lock for synchronization
        self._received_messages = list()
        self._server_lock = threading.Lock()
        self._server = ServerThread(lock = self._server_lock, recd = self._received_messages, node_id = self.node_id)
        self._server.start()

        # client params/variables
        self._send_queue = Queue.Queue()
        # read other config information from the config file and set the following values
        self._message_process_thread = ClientThread(self.node_id, self._send_queue)
        self._message_process_thread.start()

    def join(self, timeout=None):
        self._stoprequest.set()
        super(MessagingLayer, self).join(timeout)

    def run(self):
        while not self._stoprequest.isSet():
            with self._message_lock:
                if self._send_message_queue:
                    message = self._send_message_queue.pop(0)
                    self._send_message(*message)

            recv_msg = None
            with self._server_lock:
                if self._received_messages:
                    recv_msg = self._received_messages.pop(0)

            if recv_msg:
                (sender, msg) = pickle.loads(recv_msg)
                self._received_message_queue.append((sender, msg))
                self._print_message_log(recv_msg)

    def _print_message_log(self, data):
        (sender, msg) = pickle.loads(data)
        now = time.time()
        if log_messages:
            print "Received a message from " + str(sender) + ", system time is " + str(now)


    def _max_delay(self, source_id, dest_id):
        """Returns the maximum delay in the channel from the sender to the current node as a string"""
        # Assuming each node has a receving delay.
        # A two dimensional channel wise delay could be implemented here. Information about the same should be provided for the same in config.py.

        #return str(max_delays[dest_id])
        # @magicconstant - change man!!
        return '0.01'


    # Helper methods
    def _send_message(self, node_id, msg):
        current_time = time.time()
        # @implement - Currently crashing if the destination doesnot exist. Change that!!
        if log_messages:
            print "Attempting to send message \"" + str(msg) + "\" at " + '%.6f'% current_time + ' to ' + str(node_id)
        max_delay = self._max_delay(self.node_id, node_id)
        delay = random.random()*float(max_delay)

        # If this message were only message to be sent
        approximate_delivary_time = current_time + delay
        message = Message(node_id, msg, approximate_delivary_time)

        self._send_queue.put(message)

    def stop(self):
        """Stop server, client and the current message layer"""
        self._server.join()
        self._message_process_thread.join()
        self.join()
