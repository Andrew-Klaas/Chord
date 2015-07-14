import node
import random
import time
p = 4
f = 64
node_list = list()
coordinator = node.coord
#random.seed(1)
num_bits = 4
num_nodes = 2**num_bits
total_messages_p1 = 0
total_messages_p2 = 0

#phase 1
for i in range(0,p):
    while 1:
        random_node = random.randrange(0,num_nodes,1)
        if random_node not in node_list:
            node_list.append(random_node)
            # add node and count messages
            coordinator.join_node(random_node)
            break

for nodes in coordinator.nodes.keys():
    total_messages_p1 = total_messages_p1 +  int(coordinator.get_send_message_count(nodes))

#reset node message counters
coordinator.reset_send_message_count_all()


#phase 2
for i in range(0, f):
    p = random.choice(node_list)
    k = random.randrange(0,num_nodes,1)
    coordinator.find_key(p,k)

time.sleep(5)

print "total number of messages from phase 1 is: " + str(total_messages_p1)



for nodes in coordinator.nodes.keys():
    total_messages_p2 = total_messages_p2 +  coordinator.nodes[nodes].send_message_count

print "total number of messages from phase 2 is: " + str(total_messages_p2)


