import matplotlib.pyplot as plt
import json
marker = '.-'

db = json.loads(open('data.json').read())

num_nodes = db['num_nodes']
ranks_per_node = db['ranks_per_node']


def overall(kind):
    plt.clf()

    for rpn in ranks_per_node:
        num_ranks = map(lambda x: x*rpn, num_nodes)

        times = db['tests'][kind][str(rpn)]
        plt.plot(num_nodes, times, marker, label="%d ranks per node" % rpn)

    plt.title("%s Performance" % kind)
    plt.xlabel("# Blue Gene/Q nodes")
    plt.ylabel("Execution Time (seconds)")
    plt.legend(loc='upper right', shadow=True, fontsize='small')
    kind = kind.replace(" ", "_")
    plt.savefig("%s_overall.png" % kind)


def individual(kind):

    for rpn in ranks_per_node:
        plt.clf()
        num_ranks = map(lambda x: x*rpn, num_nodes)

        times = db['tests'][kind][str(rpn)]
        plt.plot(num_ranks, times, marker)

        plt.title("%s - %d ranks per node" % (kind, rpn))
        plt.xlabel("# MPI Ranks")
        plt.ylabel("Execution Time (seconds)")
        skind = kind.replace(" ", "_")
        plt.savefig("%s_%d_rpn.png" % (skind, rpn))

if __name__ == '__main__':
    for kind in db['tests']:
        print kind
        overall(kind)
        individual(kind)
