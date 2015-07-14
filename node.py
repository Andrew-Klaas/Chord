# Comments types
# @checklater
from optparse import OptionParser
from msglyr import *

class ChordNode(threading.Thread):
    """
    An abstraction of a node for the p2p network implementing the Chord protocol.
    For the purposes of this project, each node is started in a new thread (initialised and run) but it could be run as is in a general distributed setting.
    """

    def __init__(self, node_id):
        # Node params/variables
        super(ChordNode, self).__init__()
        self._stoprequest = threading.Event()
        self.node_id = node_id
        self.send_message_queue = list()        # Each element is tuple - (dest, msg)
        self.received_message_queue = list()
        self.message_lock = threading.Lock()        # Lock object to enforce synchronisation on send and receive message queues

        # Initialising the messaging layer for this node
        self.msg_layer = MessagingLayer(self.node_id, self.send_message_queue, self.received_message_queue, self.message_lock)
        self.msg_layer.start()

        self.send_message_count = 0

        self.predecessor = None
        self.successor = None

        self.finger = []
        self.finger.append(None)
        # finger table is a list of dictionaries with keys - 'start' and 'node'.
        # the first element of the fingertable is None so as to be consistent with the algorithm presented in Chord paper i.e. index starts at 1
        for i in range(NUM_IDENTIFIER_BITS):
            self.finger.append({  'start' : (self.node_id + 2**i) % MAX_NODES,
                                  'node'  : None})

        # list of all the keys stores at the current node
        self.keys = []
        self._leave = False

        if self.node_id == 0:
            self.predecessor = 0
            self.successor = 0
            for i in range(NUM_IDENTIFIER_BITS):
                self.finger[i+1]['node'] = 0
            # all the keys are initially at node 0
            self.keys.extend(range(2**NUM_IDENTIFIER_BITS))
        else:
          self.join_network()


    def join(self, timeout=None):
        self._stoprequest.set()
        super(ChordNode, self).join(timeout)

    def send(self, dest_id, msg):
        '''
        Send messages to other members of the network.
        dest_id - An integer between 0 and 2^NUM_IDENTIFIER_BITS
        msg     - Any python object to be sent. Make sure its not very large (subjective) i.e. smaller than 4096 bytes
        '''
        with self.message_lock:
            self.send_message_queue.append((dest_id, msg))
            self.send_message_count += 1


    def wait_for_reply(self, sender_node, reply_string):
        '''
        returns the first message with the value of 'reply_string' from node 'sender'
        '''
        while(1):
            with self.message_lock:
                for i in range(len(self.received_message_queue)):
                    (sender, msg) = self.received_message_queue[i]
                    if sender == sender_node:
                        if isinstance(msg, str):
                            msg_list = msg.split(' ')
                            if msg_list[0] == reply_string:
                                self.received_message_queue.pop(i)
                                return msg


    def find_successor(self, entity_id):
        pred = self.find_predecessor(entity_id)
        if pred == self.node_id:
            return self.finger[1]['node']

        self.send(pred, "get_successor")
        # the reply should be of the form 'successor value'. So extract the word in the reply
        reply = self.wait_for_reply(pred, "successor").split(' ')
        return int(reply[1])

    def is_in_interval(self, entity_id, left_index, right_index, left_inclusive, right_inclusive):
        '''
        Finds if a given entity_id is in the interval between left_index and right_index on a circle.
        left_inclusive and right_inclusive indicate whether the interval is inclusive of the left and right index
        '''
        if left_index > entity_id:
            entity_id += MAX_NODES

        if right_index <= left_index:
            right_index += MAX_NODES     # which is 256 for 8 bits

        left_cond = False
        if left_inclusive:
            left_cond = (entity_id >= left_index)
        else:
            left_cond = (entity_id > left_index)

        right_cond = False
        if right_inclusive:
            right_cond = (entity_id <= right_index)
        else:
            right_cond = (entity_id < right_index)

        if left_cond and right_cond:
            return True
        else:
            return False

    def find_predecessor(self, entity_id):
        if log_messages:
            print "find_predecessor called at " + str(self.node_id) + "; The entity_id is " + str(entity_id)
        if self.is_in_interval(entity_id, self.node_id, self.finger[1]["node"], False, True):
            return self.node_id

        if self.node_id == self.finger[1]["node"]:
            return self.node_id

        n = self.closest_preceding_finger(entity_id)
        n_successor = None

        if n != self.node_id:
            self.send(n, "get_successor")
            # the reply should be of the form 'successor value'. So extract the word in the reply
            reply = self.wait_for_reply(n, "successor").split(' ')
            n_successor = int(reply[1])
        else:
            n_successor = self.finger[1]["node"]

        while not (self.is_in_interval(entity_id, n, n_successor, False, True)):
            if n != self.node_id:
                self.send(n, "closest_preceding_finger " + str(entity_id))
                # the reply should be of the form 'closest_preceding_finger value'. So extract the word in the reply
                reply = self.wait_for_reply(n, "closest_preceding_finger").split(' ')
                #update n
                n = int(reply[1])
            else:
                n = self.closest_preceding_finger(entity_id)
            if n != self.node_id:
                self.send(n, "get_successor")
                # the reply should be of the form 'successor value'. So extract the word in the reply
                reply = self.wait_for_reply(n, "successor").split(' ')
                n_successor = int(reply[1])
            else:
                n_successor = self.finger[1]["node"]

        return n


    def closest_preceding_finger(self, entity_id):
        for i in range(NUM_IDENTIFIER_BITS, 0, -1):
            if (self.is_in_interval(self.finger[i]['node'], self.node_id, entity_id, False, False)):
                return self.finger[i]['node']
        return self.node_id


    def init_finger_table(self):

        #  update first spot in finger table
        self.send(0, "find_successor " + str(self.finger[1]["start"]))
        msg = self.wait_for_reply(0, "find_successor")
        msg_list = msg.split(' ')
        if log_messages:
            print msg
        self.finger[1]["node"] = int(msg_list[1])

        # set local predecessor
        self.send(self.finger[1]["node"], "get_predecessor")
        msg = self.wait_for_reply(self.finger[1]['node'], "predecessor")
        msg_list = msg.split(' ')
        self.predecessor = int(msg_list[1])

        # set remote  successor's predecessor to us now
        self.send(self.finger[1]['node'], "set_predecessor " + str(self.node_id))

        for i in range(1,NUM_IDENTIFIER_BITS):
            # @checklater
            if  self.is_in_interval(self.finger[i + 1]['start'], self.node_id, self.finger[i]['node'], True, True):
                self.finger[i+1]['node'] = self.finger[i]['node']
            else:
                self.send(0, "find_successor " + str(self.finger[i+1]['start']))
                msg = self.wait_for_reply(0, "find_successor")
                msg_list = msg.split(' ')
                self.finger[i + 1]['node'] = int(msg_list[1])


    def update_others(self, leave):
        for x in range(1, NUM_IDENTIFIER_BITS + 1):
            # find potential predecessors that need to be changed
            # @checklater add 1 to (n - 2^(i-1))
            desired_node = (self.node_id - 2**(x-1) + 1) % MAX_NODES
            if log_messages:
                print "desired_node - " + str(desired_node)
            p = self.find_predecessor(desired_node)

            # update their finger tables
            if leave:
                #print "leave: sending update_finger_table to " + str(p) + " at index " + str(x)

                # If the node is leaving, it  sends a new message
                # update_finger_table_leave(my_id, my_successor, finger_index)
                self.send(p, "update_finger_table_leave " + str(self.node_id) + " " + str(self.finger[1]["node"]) + " " + str(x))
            else:
                self.send(p, "update_finger_table " + str(self.node_id) + " " + str(x))

    def update_finger_table_leave(self, leave_node_id, s, i):
      # If the current ith finger of the node is leave_node_id, then set its successor as the ith finger
      if self.finger[i]["node"] == leave_node_id:
        self.finger[i]["node"] = s
        p = self.predecessor
        self.send(p, "update_finger_table_leave " + str(leave_node_id) + " " + str(s) + " " + str(i))


    def update_finger_table(self, s, i):
        # changing the implementation of update_finger_table here.
        # check if s is closer to finger[i]["start"] than finger[i]["node"]
        if (s - self.finger[i]["start"]) % MAX_NODES < (self.finger[i]["node"] - self.finger[i]["start"]) % MAX_NODES:
            self.finger[i]["node"] = s
            p = self.predecessor
            self.send(self.predecessor, "update_finger_table " + str(s) + " " + str(i))

    def join_network(self):
        self.init_finger_table()
        self.update_others(False)
        self.move_keys()

    def transfer_keys(self):
        '''
        When the node is leaving the network, this method transfers all its keys to its successor
        '''
        if len(self.keys) > 0:
            self.send(self.finger[1]["node"], "transfer_keys " + pickle.dumps(self.keys))

    def move_keys(self):
        '''
        When the node is created, it asks it successor to transfer the relevant keys the successor has to this node
        '''
        successor = self.finger[1]["node"]
        self.send(successor, "move_keys " + str(self.node_id))
        msg = self.wait_for_reply(successor, "return_keys")
        keys_index_in_cmd = msg.find(' ')
        new_keys = pickle.loads(msg[keys_index_in_cmd+1:])
        self.keys.extend(new_keys)

    def stop(self):
        """Stop server, client and the current message layer"""
        if self._leave:
            self.msg_layer.stop()
            self.join()
            return 0
        else:
            return -1

    def clean(self):
        '''
        An experimental clean method which clears all its data.
        Not required!
        '''
        self.predecessor = None
        self.successor = None

        self.finger = []
        self.finger.append(None)
        # finger table is a list of dictionaries with keys - 'start' and 'node'.
        # the first element of the fingertable is None so as to be consistent with the algorithm presented in Chord paper i.e. index starts at 1
        for i in range(NUM_IDENTIFIER_BITS):
            self.finger.append({  'start' : (self.node_id + 2**i) % MAX_NODES,
                                  'node'  : None})

        # list of all the keys stores at the current node
        self.keys = []

    def run(self):
        while not self._stoprequest.isSet():
            msg_tuple = None

            with self.message_lock:
                if self.received_message_queue:
                    msg_tuple = self.received_message_queue.pop(0)
            if msg_tuple:
                (sender, msg) = msg_tuple
                if isinstance(msg, str):
                    msg_list = msg.split(' ')
                    cmd = msg_list[0]


                    if log_messages:
                        print("Run: " + msg + " from " + str(sender))
                    # Different commands/requests handled here
                    if cmd == "update_finger_table":
                        self.update_finger_table(int(msg_list[1]), int(msg_list[2]))

                    elif cmd == "update_finger_table_leave":
                        self.update_finger_table_leave(int(msg_list[1]), int(msg_list[2]), int(msg_list[3]))

                    elif cmd == "find_successor":
                        s = self.find_successor(int(msg_list[1]))
                        self.send(sender, "find_successor " + str(s))

                    elif cmd == "find_predecessor":
                        s = self.find_predecessor(int(msg_list[1]))
                        self.send(sender, "find_predecessor " + str(s))

                    elif cmd == "closest_preceding_finger":
                        s = self.closest_preceding_finger(int(msg_list[1]))
                        self.send(sender, "closest_preceding_finger " + str(s))

                    elif cmd == "get_successor":
                        self.send(sender, "successor " + str(self.finger[1]['node']))

                    elif cmd == "get_predecessor":
                        self.send(sender, "predecessor " + str(self.predecessor))

                    elif cmd == "set_predecessor":
                        self.predecessor = int(msg_list[1])

                    elif cmd == "leave":
                        self.transfer_keys()

                        # order in which these messages are sent matters to avoid predecessor sending update_finger_table_leave to this node which would have probably left by then
                        # set the predecessor of this node's successor to this node's predecessor
                        self.send(self.finger[1]["node"], "set_predecessor " + str(self.predecessor))


                        self.update_others(True)

                        # Note that all the 3 method calls above would not generate a request to this node. So, this node no longer needs to wait and can leave gracefully immediately
                        self._leave = True
                        #self.stop()

                    elif cmd == "transfer_keys":
                        keys_index_in_cmd = msg.find(' ')
                        new_keys = pickle.loads(msg[keys_index_in_cmd+1:])
                        self.keys.extend(new_keys)
                        self.keys.sort()

                    elif cmd == "show":
                        print self.keys

                    elif cmd == "find":
                        s = self.find_successor(int(msg_list[1]))
                        #if log_messages:
                        print "key " + msg_list[1] + " is with " + str(s)

                    elif cmd == "move_keys":
                        new_node = int(msg_list[1])
                        len_keys = len(self.keys)
                        new_key_indices = [i for i in range(len_keys) if ((new_node - self.keys[i]) % MAX_NODES < (self.node_id - self.keys[i]) % MAX_NODES)]
                        new_keys = [self.keys[i] for i in new_key_indices]
                        for i in reversed(new_key_indices):
                            self.keys.pop(i)
                        self.send(sender, "return_keys " + pickle.dumps(new_keys))

                    elif cmd == "get_send_message_count":
                        if log_messages:
                            print "here!!!!.... " + str(self.node_id) + "\n"
                        self.send(sender, "send_message_count " + str(self.send_message_count))

                    elif cmd == "reset_send_message_count":
                        self.send_message_count = 0

                    elif cmd == "get_keys":
                        self.send(sender, "keys " + pickle.dumps(self.keys))


class Coordinator(object):
    """
    The coordinator for the interfacing with the chord network. Processes different commands from the command line and send appropriate messages to the ChordNodes.
    """

    def __init__(self, node_id):
        # Node params/variables
        self.node_id = node_id
        self.send_message_queue = list()        # Each element is tuple - (dest, msg)
        self.received_message_queue = list()
        self.message_lock = threading.Lock()        # Lock object to enforce synchronisation on send and receive message queues

        # Initialising the messaging layer for this node
        self.msg_layer = MessagingLayer(self.node_id, self.send_message_queue, self.received_message_queue, self.message_lock)
        self.msg_layer.start()
        node_0 = ChordNode(0)
        node_0.start()

        # References maintained only to stop the thread. All the commands are send through network as messages
        self.nodes = {}
        self.nodes[0] = node_0

        self.out_file = None

    def send(self, dest_id, msg):
        '''
        Send messages to other members of the network.
        dest_id - An integer between 0 and 2^NUM_IDENTIFIER_BITS
        msg     - Any python object to be sent. Make sure its not very large (subjective) i.e. smaller than 4096 bytes
        '''
        with self.message_lock:
            self.send_message_queue.append((dest_id, msg))


    def wait_for_reply(self, sender_node, reply_string):
        '''
        returns the first message with the value of 'reply_string' from node 'sender'
        '''
        while(1):
            with self.message_lock:
                for i in range(len(self.received_message_queue)):
                    (sender, msg) = self.received_message_queue[i]
                    if sender == sender_node:
                        if isinstance(msg, str):
                            msg_list = msg.split(' ')
                            if msg_list[0] == reply_string:
                                self.received_message_queue.pop(i)
                                return msg

    def process_input_commands(self, file):
      '''Processes an input containing the commands and delays or an infinite loop processing inputs'''
      if file:
        with open(commands_file[self.node_id]) as f:
          for line in f:
            print node_names[self.node_id] + ' >> ' + line
            line = line.strip()
            self.process_command(line.split(' '))
      else:
        print "wait for # to show up before entering the next command"
        while(1):
            sys.stdout.write("#  ")
            cmd = raw_input().split(' ')
            if self.process_command(cmd) == 1:
                break
            else:
                time.sleep(0.2)


    # Process command methods goes here
    def reset_send_message_count_all(self):
        for node_id in self.nodes:
            self.send(node_id, "reset_send_message_count")

    def get_send_message_count(self, node_id):
        if node_id not in self.nodes.keys():
            return -1
        self.send(node_id, "get_send_message_count")
        reply = self.wait_for_reply(node_id, "send_message_count")
        if log_messages:
            print "HELLLOO"
        return reply.split(' ')[1]

    def join_node(self, new_node_id):
        if new_node_id not in self.nodes.keys():
            new_node = ChordNode(new_node_id)
            new_node.start()
            self.nodes[new_node_id] = new_node

    def leave_node(self, node_id):
        if node_id in self.nodes.keys():
            self.send(node_id, "leave")
            # @checklater - stop getting called before the leave executes or something - Hey! Fixed it \m/
            while self.nodes[node_id].stop() != 0:
                pass
            self.nodes.pop(node_id, None)

    def find_key(self, node_id, key_id):
        if node_id in self.nodes.keys():
            self.send(node_id, "find " + str(key_id))

    def clear_out_file(self):
        if self.out_file:
            open(self.out_file, 'w').close()

    def show_keys(self, node_id):
        if node_id in self.nodes.keys():
            keys = self.get_keys(node_id)
            keys = map(str, keys)
            output_string = str(node_id) + ' ' + ' '.join(keys)
            #print output_string
            if self.out_file:
                with open(self.out_file, 'a') as f:
                    f.write(output_string + '\n')

            else:
                #self.send(node_id, "show")
                print output_string

    def get_keys(self, node_id):
        self.send(node_id, "get_keys")
        if node_id in self.nodes:
            reply = self.wait_for_reply(node_id, "keys")
            keys_index_in_cmd = reply.find(' ')
            keys = pickle.loads(reply[keys_index_in_cmd+1:])
            return keys
        else:
            return None

    def show_all(self):
        nodes = self.nodes.keys()
        nodes.sort()
        for node in nodes:
            self.show_keys(node)

    def process_command(self, cmd):
      if cmd[0].lower() == "stop":
        return 1

      elif cmd[0].lower() == "join":
        new_node_id = int(cmd[1])
        self.join_node(new_node_id)
        time.sleep(2)

      elif cmd[0].lower() == "find":
        node_id = int(cmd[1])
        key_id = int(cmd[2])
        self.find_key(node_id, key_id)

      elif cmd[0].lower() == "leave":
        node_id = int(cmd[1])
        self.leave_node(node_id)
        time.sleep(2)

      elif cmd[0].lower() == "show":
        node_id = int(cmd[1])
        self.show_keys(node_id)

      elif cmd[0].lower() == "show-all":
        self.show_all()

      else:
        print "No command " + cmd[0] + " found."

#coord_t = Coordinator(test_id)  # cannot have two co-ordinator initialising node 0 as only one can get the port

coord = None

if __name__ == "__main__":
    print sys.argv

    parser = OptionParser()
    parser.add_option("-g", "--file", dest="filename",
                      help="write show results to FILE", metavar="FILE")

    (options, args) = parser.parse_args()
    # options is some weird Values type!

    coord = Coordinator(coordinator_id)

    if options.filename:
        coord.out_file = options.filename

    coord.process_input_commands(0)

else:
    # Start a coordinator anyway
    coord = Coordinator(coordinator_id)



# gen code to create 30 nodes

# clear any showresults stuff
coord.clear_out_file()

def test():
    should_add = int(random.random()*2)
    if should_add:
        # add 3 nodes
        count = 0
        while count < 3:
            node_id = int(random.random()*MAX_NODES)
            if node_id in coord.nodes:
                pass
            else:
                print "join - " + str(node_id)
                coord.join_node(node_id)
                time.sleep(0.1)
                count +=1
        print "added 3 nodes"
    else:
        if len(coord.nodes.keys()) == 1:
            return
        count = 0
        while count < 1:
            node_id = random.choice(coord.nodes.keys())
            if node_id == 0:
                continue
            print "leave " + str(node_id)
            time.sleep(0.2)
            coord.leave_node(node_id)
            count += 1
        print "removed 1 node"

'''
for i in range(20):
    test()
    coord.out_file = "showresults" + str(i)
    coord.clear_out_file()
    coord.show_all()
'''
