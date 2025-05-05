import multiprocessing as mp
import pandas as pd
import matplotlib.pyplot as plt
from utils.recency import LRUCache
from utils.frequency import LFUCacheList
from utils.checkpoint import load_json, save_json

def estimator(df, block_rank, ref_cnt):
    for index, row in df.iterrows():  ### one by one
        ### Increase readcnt/writecnt by matching 'type' and block_rank
        acc_rank = block_rank.reference(row['blocknum'])
        if acc_rank == -1:
            continue
        else:
            try:
                ref_cnt[acc_rank] += 1  # Increase [acc_rank]th element of readcnt by 1
            except IndexError:  # ***list index out of range
                for i in range(len(ref_cnt), acc_rank + 1):
                    ref_cnt.insert(i, 0)
                ref_cnt[acc_rank] += 1

    return block_rank, ref_cnt


def mp_estimator(ref_block, startpoint, endpoint_q, input_filename, output_filename):
    block_rank = list()
    ref_cnt = list()

    if (startpoint > 0):
        filename = output_filename + "_checkpoint" + str(startpoint - 1) + ".json"
        saving_list = ['block_rank', 'ref_cnt']

        block_rank, ref_cnt = load_json(saving_list, filename)
        ref_block.set(block_rank)
        # print(block_rank, ref_cnt)

    i = startpoint
    while True:
        if not startpoint:
            df = pd.read_csv(input_filename, sep=',', header=0, index_col=None, on_bad_lines='skip')
        else:
            try:
                df = pd.read_csv(input_filename + '_' + str(i), sep=',', header=0, index_col=0, on_bad_lines='skip')
            except FileNotFoundError:
                print("no file named:", input_filename + '_' + str(i))
                break

        ref_block, ref_cnt = estimator(df, ref_block, ref_cnt)
        block_rank = ref_block.get()

        if not startpoint:
            filename = output_filename + ".json"
        else:
            filename = output_filename + "_checkpoint" + str(i) + ".json"
        savings = {'block_rank': block_rank, 'ref_cnt': ref_cnt}
        save_json(savings, filename)

        if not startpoint:
            break
        else:
            i += 1

    endpoint_q.put(i)    # return i

def estimator_run(estimator_type, start_chunk, input_filename, output_filename):
    endpoint_q = mp.Queue()
    processes = []
    endpoints, ref_cnts = [], []

    assert (estimator_type == 'recency' or estimator_type == 'frequency')

    if not start_chunk:
        suffix = "-"+estimator_type+"_estimator"
    else:
        end_chunk = endpoints[0]
        suffix = "_checkpoint" + str(end_chunk) + "-"+estimator_type+"_estimator"

    if estimator_type == 'recency':
        ref_block = LRUCache()
    else:
        ref_block = LFUCacheList()

    p = mp.Process(target=mp_estimator, args=(ref_block, start_chunk, endpoint_q, input_filename, output_filename+suffix))
    processes.append(p)
    p.start()

    # get return value
    for p in processes:
        endpoints.append(endpoint_q.get())

    for p in processes:
        p.join()

    filename = output_filename + suffix + ".json"
    _, ref_cnt = load_json(['block_rank', 'ref_cnt'], filename)
    ref_cnts.append(ref_cnt)

def estimator_graph(recency_cnt, frequency_cnt, title, filename, xlim : list = None, ylim : list = None):
    #fig, ax = plot_frame((1, 1), title=title, xlabel='File block rank', ylabel='Reference counts', log_scale=False)
    plt.rc('font', size=20)
    fig, ax = plt.subplots(1,1, figsize=(7,7), constrained_layout=True)
    ax.set_xlabel('File block rank', fontsize=25)
    ax.set_ylabel('Reference counts', fontsize=25)

    ax.set_axisbelow(True)
    ax.grid(True, which='major', color='black', alpha=0.5, linestyle='--')
    ax.grid(True, which='minor', color='black', alpha=0.3, linestyle='--', lw=0.3)
    #ax.grid(axis='y', which='minor', color='black', alpha=0.3, linestyle='--', lw=0.3)
    plt.xscale('log')
    plt.yscale('log')

    if xlim:
        plt.setp(ax, xlim=xlim)
    if ylim:
        plt.setp(ax, ylim=ylim)

    #recency
    x1 = range(1,len(recency_cnt)+1)
    y1 = recency_cnt

    #frequency
    x2 = range(1,len(frequency_cnt)+1)
    y2 = frequency_cnt

    # colors: ['royalblue', 'crimson'], ['#006b70', '#ff7c00'], ['purple', 'darkgreen']
    ax.scatter(x1, y1, color='#006b70', alpha=0.7, marker='o', label='recency')       # recency graph
    ax.scatter(x2, y2, color='#ff7c00', alpha=0.7, marker='s', label='frequency')     # frequency graph

    # legend
    ax.legend(loc=(0.075, 1.01), ncol=2, fontsize=20, markerscale=3)  # loc='upper right', ncol=1

    #plt.show()
    plt.savefig(filename+'-estimator.png', dpi=300)

#----------------
if __name__=="__main__":
    # add parser
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", "-i", metavar='I', type=str,
                        nargs='?', default='trace.csv', help='input file path')
    parser.add_argument("--output", "-o", metavar='O', type=str,
                        nargs='?', default='output', help='output file path')
    parser.add_argument("--title", "-t", metavar='T', type=str,
                        nargs='?', default='', help='title of figures')
    args = parser.parse_args()

    #-----
    suffix = "_estimator"
    for et in ['recency', 'frequency']:
        estimator_run(estimator_type=et, start_chunk=0, input_filename=args.input, output_filename=args.output)

    recency_filename = args.output + '-recency' + suffix + '.json'
    _, recency_ref_cnt = load_json(['block_rank', 'ref_cnt'], recency_filename)

    frequency_filename = args.output + '-frequency' + suffix + '.json'
    _, frequency_ref_cnt = load_json(['block_rank', 'ref_cnt'], frequency_filename)

    estimator_graph(recency_cnt=recency_ref_cnt, frequency_cnt=frequency_ref_cnt, title=args.title, filename=args.output)
