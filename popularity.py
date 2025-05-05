import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math

def ref_cnt_per_block(df_list):
    df = pd.DataFrame()
    df_rw = pd.DataFrame()
    for i in range(len(df_list)):
        cur_df = df_list[i].groupby(['blocknum', 'operation'])['blocknum'].count().reset_index(name='count')
        df = pd.concat([df, cur_df])

    # reduce sum
    df = df.groupby(by=['blocknum', 'operation'], as_index=False).sum()

    return df

#-----
def ref_cnt_percentile_rank(df):
    total_read = df['count'][(df['operation'] == 'read')].sum()
    total_write = df['count'][(df['operation'] == 'write')].sum()

    # percentage
    df['op_pcnt'] = df['count'].astype('float64')
    df.loc[(df['operation'] == 'read'), ['op_pcnt']] /= total_read
    df.loc[(df['operation'] == 'write'), ['op_pcnt']] /= total_write

    # ranking in percentile form
    read_rank = df['op_pcnt'][(df['operation'] == 'read')].rank(ascending=False, pct=True)
    df.loc[(df['operation'] == 'read'), ['op_pcnt_rank']] = read_rank

    write_rank = df['op_pcnt'][(df['operation'] == 'write')].rank(ascending=False, pct=True)
    df.loc[(df['operation'] == 'write'), ['op_pcnt_rank']] = write_rank

    return df

#-----
def cdf_graph(df, fig_title, filename):
    #fig, ax = plot_frame((1, 1), title=fig_title, xlabel='Rank by reference count (%)', ylabel='Cumulative access ratio (%)')
    plt.rc('font', size=20)
    fig, ax = plt.subplots(1,1, figsize=(7,7), constrained_layout=True)
    ax.set_xlabel('Rank by reference count (%)', fontsize=25)
    ax.set_ylabel('Cumulative access ratio (%)', fontsize=25)

    ax.set_axisbelow(True)
    ax.grid(True, color='black', alpha=0.5, linestyle='--')

    # calculate CDF for each operation
    x_list, y_list = [], []
    operations = ['read', 'write']
    for op in operations:
        cur_cdf = df['op_pcnt'][(df['operation'] == op)].sort_values(ascending=False).cumsum().to_numpy()
        cur_cdf_rank = df['op_pcnt_rank'][(df['operation'] == op)].sort_values(ascending=True).to_numpy()
        cur_x = np.concatenate(([0, cur_cdf_rank[0]], cur_cdf_rank, [1]))
        cur_y = np.concatenate(([0, 0], cur_cdf, [1]))

        x_list.append(cur_x)
        y_list.append(cur_y)

    # plot
    colors = ['blue', 'red']
    dash_colors = ['darkblue', 'brown']
    labels = ['read', 'write']
    for i in range(len(operations)):
        x_l = np.arange(len(x_list[i])-3) / (len(x_list[i])-3) * 100
        y_l = y_list[i][2:-1] * 100

        ax.plot(x_l, y_l, color=dash_colors[i], label=labels[i], lw=3)

    # legend
    ax.legend(loc='lower right', ncol=1, fontsize=20)

    #plt.show()
    plt.savefig(filename+'_cdf.png', dpi=300)

if __name__ == '__main__':
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

    # check if the output path exists
    import os
    if not os.path.exists(args.output):
        os.makedirs(args.output)
        print(f"Make directory: {args.output}")

    df_chunk = pd.read_csv(args.input, sep=',', chunksize=1000000, header=0, index_col=0, on_bad_lines='skip')
    df1 = ref_cnt_per_block(df_list=list(df_chunk))

    df2 = ref_cnt_percentile_rank(df1)
    cdf_graph(df=df2, fig_title=args.title, filename=args.output)