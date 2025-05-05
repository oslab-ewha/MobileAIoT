from utils.filecache import FileCache
import math, operator
import pandas as pd
import time

def simulation(df, size, max_buffer, ratio):
    flush_dict = {}
    flush_rtime, flush_period, last_rtime, flush_cnt = 0, 5, 0, 0

    #--------------------------------
    s = FileCache(max_cache_size=size, write_buffer_max=max_buffer, ratio=ratio)
    for index, row in df.iterrows():
        if row[1] >= flush_period and (int(row[1] - flush_rtime) >= flush_period or int(row[1] - last_rtime) >= flush_period):
            f = s.flush(cur_vtime=index, cur_rtime=flush_rtime+flush_period)
            if f != -1:
                flush_dict[flush_rtime+flush_period] = f
            flush_cnt += 1
            flush_rtime = math.floor(row[1]) - (math.floor(row[1]) % flush_period)

        s.reference(cur_vtime=index, cur_rtime=row[1], operation=row[3], blknum=row[4], inode=row[5])

        last_rtime = row[1]

    f = s.flush(cur_vtime=index+1, cur_rtime=last_rtime)
    flush_cnt += 1
    if f != -1:
        flush_dict[last_rtime] = f

    print(s.stor_flush_cnt, len(s.write_buffer.cache), sep=",\t")


def simulation_run():
    PATH = 'trace.csv'
    df = pd.read_csv(PATH, header=None, skiprows=1)
    SIZE, B_SIZE = len(df[4].unique()), len(df[df[3]=='write'][4].unique())

    print("write buffer ratio,\tstorage write count,\twrite buffer block count")
    for r in [i / 20 for i in range(1,11)]:
        print(r, end=",\t")
        simulation(df, size=SIZE, max_buffer=B_SIZE, ratio=r)

if __name__ == "__main__":
    simulation_run()
