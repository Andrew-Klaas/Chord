from optparse import OptionParser
from config import *

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-g", "--file", dest="filename",
                      help="write show results to FILE", metavar="FILE")

    (options, args) = parser.parse_args()

    results = {}
    is_correct = False

    if options.filename:
        with open(options.filename, 'r') as f:
            for line in f:
                line = line.split(' ')
                results[int(line[0])] = map(int, line[1:])


    node_ids = results.keys()
    node_ids.sort()
    last_node = node_ids[-1]
    required_range = range(last_node+1, MAX_NODES)
    required_range.append(0)
    if set(results[0]) == set(required_range):
        pass #print "Yo dude!"

    for i in range(1, len(node_ids)):
        current_node_id = node_ids[i]
        predecessor = node_ids[i-1]
        required_range = range(predecessor + 1, current_node_id + 1)
        if set(results[current_node_id]) == set(required_range):
            pass #print "Yo dude!"
        else:
            print "Shit broke at node - %d" + current_node_id
            break

    print "All is well \m/"
