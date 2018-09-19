#!/usr/bin/env python
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

import pandas as pd
import smtplib
from tabulate import tabulate
import sys

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
     %s
    </p>
  </body>
</html>
"""


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


def send_email(html_msg):
    """
    type html: string
    """
    message = MIMEMultipart('peloton_performance')
    message['Subject'] = "Peloton Performance"
    message['From'] = FROM
    message['To'] = TO

    html = MIMEText(html_msg, 'html')
    message.attach(html)

    print "Emailing performance report to " + TO
    s = smtplib.SMTP('localhost')
    s.sendmail(FROM, [TO], message.as_string())
    s.quit()


def main():
    args = parse_arguments(sys.argv[1:])

    perf_file_1 = PERF_DIR + '/' + args.file_1
    perf_file_2 = PERF_DIR + '/' + args.file_2

    df1 = pd.read_csv(perf_file_1, '\t', index_col=0)
    df2 = pd.read_csv(perf_file_2, '\t', index_col=0)
    merge_table = perf_compare(df1, df2)
    print tabulate(merge_table, headers='keys', tablefmt='orgtbl')
    merge_table.to_csv('compare.csv', sep='\t')

    table_html = merge_table.to_html()
    commit_info = os.environ.get(COMMIT_INFO) or 'N/A'
    build_url = os.environ.get(BUILD_URL) or 'N/A'
    msg = HTML_TEMPLATE % (commit_info, build_url, table_html)

    if TO:
        send_email(msg)


if __name__ == "__main__":
    main()
