from config import *

class ServerThread(threading.Thread):
    """
    Checks for any message to be processed in the Queue (which is FIFO*) and sends it appropriately by giving it a suitable delay.

    * - assumption. Have to validate. Whole FIFO thing hinges on this.
    Ref - http://eli.thegreenplace.net/2011/12/27/python-threads-communication-and-stopping
    """

    def __init__(self, lock, recd, node_id):
        super(ServerThread, self).__init__()
        self.node_id = node_id
        self.recd = recd
        self.lock = lock
        self.count = 0
        self.stoprequest = threading.Event()

        # Initialise the server binding it to the given address
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.server_socket.bind(node_address[node_id])
        except socket.error , msg:
            print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
            sys.exit()
        # @change - Currently the listen is set to 10 - random
        self.server_socket.listen(MAX_LISTEN)
        print 'Server at the node is listening at port - ' + str(node_address[self.node_id][1])
        #@test
        global test
        test = self.server_socket

    def join(self, timeout=None):
        self.stoprequest.set()
        super(ServerThread, self).join(timeout)

    def run(self):
        while not self.stoprequest.isSet():

            try:
                conn, addr = self.server_socket.accept()

                # @change - 4096 is random here. Parametrize it
                data = conn.recv(4096)
                now = time.time()
                with self.lock:
                    self.recd.append(data)
                    #self._print_message_log(data)
            except socket.timeout:
                continue
        self.server_socket.close()
