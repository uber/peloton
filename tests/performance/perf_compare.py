#!/usr/bin/env python
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import pandas as pd
from tabulate import tabulate
import sys

PERF_DIR = "tests/performance/PERF_RES"


def parse_arguments(args):

    parser = ArgumentParser(
        description='',
        formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument(
        '-f1',
        '--file-1',
        dest='file_1',
        help='the first perf data file',
    )
    parser.add_argument(
        '-f2',
        '--file-2',
        dest='file_2',
        help='the second perf data file',
    )
    return parser.parse_args(args)


def perf_compare(df1, df2):
    """
    type df1: pandas.DataFrame
    type df2: pandas.DataFrame
    rtype pandas.DataFrame
    """
    merge_table = pd.merge(
        df1, df2,
        how='right',
        on=['Cores', 'Sleep(s)', 'UseInsConf', 'TaskNum']
    )

    merge_table['Perf Change'] = merge_table.apply(
        lambda x: format(
            (x['Exec(s)_y'] - x['Exec(s)_x']) / x['Exec(s)_x'], '.4f'),
        axis=1)
    return merge_table


def main():
    args = parse_arguments(sys.argv[1:])

    perf_file_1 = PERF_DIR + '/' + args.file_1
    perf_file_2 = PERF_DIR + '/' + args.file_2

    df1 = pd.read_csv(perf_file_1, '\t', index_col=0)
    df2 = pd.read_csv(perf_file_2, '\t', index_col=0)
    merge_table = perf_compare(df1, df2)
    print tabulate(merge_table, headers='keys', tablefmt='orgtbl')
    merge_table.to_csv('compare.csv', sep='\t')


if __name__ == "__main__":
    main()
