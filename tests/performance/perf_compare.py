#!/usr/bin/env python
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

import pandas as pd
import smtplib
import sys

from multi_benchmark import output_files_list

PERF_DIR = 'tests/performance/PERF_RES'

SERVER = 'localhost'
FROM = 'peloton.performance@uber.com'
TO = os.environ.get('EMAIL_TO', 'peloton-group@uber.com')
COMMIT_INFO = 'COMMIT_INFO'
BUILD_URL = 'BUILD_URL'

HTML_TEMPLATE = """
<html>
  <head>
    Peloton Performance Report
  </head>
  <body>
    <h1>
        Peloton Performance Report
    </h1>
    <p>
        Environment: vCluster <br>
        Commit info: %s <br>
        Build URL: %s <br>
        CPU per Node: 1 <br>
        Memory per Node: 32 MB <br>
        Disk per Node: 32 MB <br>
        Respool size: 900 Nodes <br>
    </p>
    <p>
        Performance Change on <b>Job create</b>
     %s
    </p>
    <p>
        Performance Change on <b>Job get</b>
     %s
    </p>
    <p>
        Performance Change on <b>Job update</b>
     %s
    </p>
  </body>
</html>
"""


def parse_arguments(args):

    parser = ArgumentParser(
        description='',
        formatter_class=RawDescriptionHelpFormatter)

    # Input of this argument should be either the file name, or the file
    # name without '.csv' suffix. This input will look for a set of data files
    # generated along. Input of this argument shouldn't contain '_job_get'
    # or '_job_update' segments.
    parser.add_argument(
        '-f1',
        '--file-1',
        dest='file_1',
        help='Filename prefix of baseline perf data files',
    )
    # Same as the comment for '-f1'.
    parser.add_argument(
        '-f2',
        '--file-2',
        dest='file_2',
        help='Filename prefix of perf data files to compare against baseline',
    )
    return parser.parse_args(args)


def compare_create(f1, f2):
    """
    Returns the benchmark results of comparing perf executions in
    html table format.

    Args:
        f1: pandas.DataFrame, benchmark result on one run
        f2: pandas.DataFrame, benchmark result on the second run

    Returns:
        html table object.
    """
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1, dataframe_2,
        how='right',
        on=['Cores', 'Sleep(s)', 'UseInsConf', 'TaskNum']
    )

    merge_table['Perf Change'] = merge_table.apply(
        lambda x: format(
            (x['Exec(s)_y'] - x['Exec(s)_x']) / x['Exec(s)_x'], '.4f'),
        axis=1)

    return merge_table.to_html()


def compare_get(f1, f2):
    """
    Returns the benchmark results of comparing job's GET counts in
    html table format.

    Args:
        f1: pandas.DataFrame, benchmark result on job get
        f2: pandas.DataFrame, benchmark result on job get

    Returns:
        html table object.
    """
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1, dataframe_2,
        how='right',
        on=['TaskNum', 'Sleep(s)', 'UseInsConf']
    )

    return merge_table.to_html()


def compare_update(f1, f2):
    """
    Returns the benchmark results of updating a job and its comparison in
    html table format.

    Args:
        f1: pandas.DataFrame, benchmark result on job update
        f2: pandas.DataFrame, benchmark result on job update

    Returns:
        html table object.
    """
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1, dataframe_2,
        how='right',
        on=['NumStartTasks', 'TaskIncrementEachTime', 'NumOfIncrement',
            'Sleep(s)', 'UseInsConf']
    )

    merge_table['Time Diff'] = merge_table.apply(
        lambda x: format(
            (x['TotalTimeInSeconds_y'] - x['TotalTimeInSeconds_x']), '.2f'),
        axis=1)

    return merge_table.to_html()


def send_email(html_msg):
    """
    type html: string
    """
    message = MIMEMultipart('peloton_performance')
    message['Subject'] = "Peloton Performance"
    message['From'] = FROM
    message['To'] = TO

    html = MIMEText(html_msg, 'html')

    print('The html generated is: %s', html)
    message.attach(html)

    print "Emailing performance report to " + TO
    s = smtplib.SMTP('localhost')
    s.sendmail(FROM, [TO], message.as_string())
    s.quit()


def main():
    args = parse_arguments(sys.argv[1:])

    # Aggregates data source with its function operation.
    operations = [compare_create, compare_get, compare_update]
    file_set_1 = output_files_list(PERF_DIR + '/', args.file_1)
    file_set_2 = output_files_list(PERF_DIR + '/', args.file_2)
    result_htmls = [None] * 3

    # Compare and generate html tables.
    for i, combo in enumerate(zip(operations, file_set_1, file_set_2)):
        func, f1, f2 = combo
        result_htmls[i] = func(f1, f2)

    commit_info = os.environ.get(COMMIT_INFO) or 'N/A'
    build_url = os.environ.get(BUILD_URL) or 'N/A'
    msg = HTML_TEMPLATE % (commit_info, build_url,
                           result_htmls[0], result_htmls[1], result_htmls[2])
    print(msg)
    if TO:
        send_email(msg)


# Helper function. Converts csv contents to dataframe.
def to_df(csv_file): return pd.read_csv(csv_file, '\t', index_col=0)


if __name__ == "__main__":
    main()
