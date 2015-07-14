from config import *

class ClientThread(threading.Thread):
    """
    Checks for any message to be processed in the Queue (which is FIFO*) and sends it appropriately by giving it a suitable delay.

    * - assumption. Have to validate. Whole FIFO thing hinges on this.
    Ref - http://eli.thegreenplace.net/2011/12/27/python-threads-communication-and-stopping
    """

    def __init__(self, node_id, message_queue):
        super(ClientThread, self).__init__()
        self.node_id = node_id
        self.message_queue = message_queue
        self.last_sent_time = 0
        self.stoprequest = threading.Event()

    def join(self, timeout=None):
        self.stoprequest.set()
        super(ClientThread, self).join(timeout)

    def run(self):
        while not self.stoprequest.isSet():
            try:
                message = self.message_queue.get(True, 0.05);
                delivary_time = float(message.delivary_time)
                current_time = time.time()


                if (delivary_time > self.last_sent_time):
                    if (delivary_time > current_time):
                        time.sleep(delivary_time - current_time)
                # else continue sending the message

                try:
                    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except socket.error:
                    print 'Failed to create socket'
                    sys.exit()

                try:
                    send_socket.connect(node_address[message.node_id])
                except socket.gaierror, e:
                    print "Address-related error connecting to server: %s" % e
                    #sys.exit(1)
                except socket.error, e:
                    print "Connection error: %s" % e
                    #sys.exit(1)

                try:
                    # @warning - msg_string is unfortunately a list
                    send_socket.sendall(pickle.dumps((self.node_id, message.msg)))
                    send_socket.close()
                except socket.error:
                    print "Send to %s failed" % str(message.node_id)
                    #sys.exit()

                # message sent
                self.last_sent_time = time.time()

            except Queue.Empty:
                continue
