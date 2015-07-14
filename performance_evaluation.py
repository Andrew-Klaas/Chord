from node import *

NUM_EXPERIMENTS = 1 #N
NUM_NODES_TO_ADD = 40 #P
NUM_FIND_OPERATIONS = 64 #F

phase1_messages = []
phase2_messages = []

for i in range(NUM_EXPERIMENTS):
    print "%d th experiment" % i
    random.seed(10)
    count = 0
    while count < NUM_NODES_TO_ADD:
        node_id = int(random.random()*MAX_NODES)
        if node_id in coord.nodes:
            continue
        else:
            coord.join_node(node_id)
            time.sleep(0.1)
            count += 1
            print count


    total_messages = 0
    for nodes in coord.nodes.keys():
        total_messages += int(coord.get_send_message_count(nodes))
        time.sleep(0.1)

    phase1_messages.append(total_messages)


    coord.reset_send_message_count_all()
    time.sleep(0.1)



    #phase 2
    print "phase 2"
    f = NUM_FIND_OPERATIONS
    for i in range(0, f):
        if i % 10 == 0:
            print "find operations - " + str(i)
        p = random.choice(coord.nodes.keys())
        k = random.randrange(MAX_NODES)
        coord.find_key(p,k)
        time.sleep(0.2)

    total_messages = 0
    for node_id in coord.nodes.keys():
        total_messages += int(coord.get_send_message_count(node_id))
        time.sleep(0.1)

    phase2_messages.append(total_messages)

    print "phase1_messages = " + str(phase1_messages)
    print "phase2_messages = " + str(phase2_messages)

    # ask all nodes to leave
    for node_id in coord.nodes.keys():
        if node_id == 0:
            continue
        print "leave - " + str(node_id)
        coord.leave_node(node_id)
        time.sleep(0.1)

    coord.reset_send_message_count_all()
