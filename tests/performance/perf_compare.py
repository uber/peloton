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
import report_styling as styling

PERF_DIR = "tests/performance/PERF_RES"

SERVER = "localhost"
FROM = "peloton.performance@uber.com"
TO = os.environ.get("EMAIL_TO", "peloton-group@uber.com")
COMMIT_INFO = "COMMIT_INFO"
BUILD_URL = "BUILD_URL"

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
    <p>
        Performance Change on <b>Stateless Job create</b>
     %s
    </p>
    <p>
        Performance Change on <b>Stateless Job update</b>
    %s
    </p>
    <p>
        Performance Change on <b>Parallel Stateless Job update</b>
     %s
    </p>
    <p>
        Performance Change on <b>Stateless Job host-limit-1 Create</b>
    %s
    </p>
    <p>
        Performance Change on <b>Stateless Job host-limit-1 Update</b>
    %s
    </p>
  </body>
</html>
"""


def parse_arguments(args):

    parser = ArgumentParser(
        description="", formatter_class=RawDescriptionHelpFormatter
    )

    # Input of this argument should be either the file name, or the file
    # name without '.csv' suffix. This input will look for a set of data files
    # generated along. Input of this argument shouldn't contain '_job_get'
    # or '_job_update' segments.
    parser.add_argument(
        "-f1",
        "--file-1",
        dest="file_1",
        help="Filename prefix of baseline perf data files",
    )
    # Same as the comment for '-f1'.
    parser.add_argument(
        "-f2",
        "--file-2",
        dest="file_2",
        help="Filename prefix of perf data files to compare against baseline",
    )
    return parser.parse_args(args)


"""
Returns the benchmark results of comparing perf executions in
html table format.

Args:
    base_version: Peloton perf test base verion
    current_version: current Peloton perf test version
    f1: pandas.DataFrame, benchmark result on one run
    f2: pandas.DataFrame, benchmark result on the second run

Returns:
    html table object.
"""


def compare_create(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=["Cores", "Sleep(s)", "UseInsConf", "TaskNum"],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["Exec(s)_y"] - x["Exec(s)_x"]) / float(x["Exec(s)_x"]) * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "create",
        base_version,
        current_version,
    )

    return enriched_table


"""
Returns the benchmark results of comparing job's GET counts in
html table format.

Args:
    base_version: Peloton perf test base verion
    current_version: current Peloton perf test version
    f1: pandas.DataFrame, benchmark result on job get
    f2: pandas.DataFrame, benchmark result on job get

Returns:
    html table object.
"""


def compare_get(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=["TaskNum", "Sleep(s)", "UseInsConf"],
    )

    merge_table["Get Counts Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["Gets_y"] - x["Gets_x"]) / float(x["Gets_x"]) * 100, ".2f"
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Get Counts Change (%)",
        "get",
        base_version,
        current_version,
    )

    return enriched_table


"""
    Returns the benchmark results of updating a job and its comparison in
    html table format.

    Args:
        base_version: Peloton perf test base verion
        current_version: current Peloton perf test version
        f1: pandas.DataFrame, benchmark result on job update
        f2: pandas.DataFrame, benchmark result on job update

    Returns:
        html table object.
"""


def compare_update(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumStartTasks",
            "TaskIncrementEachTime",
            "NumOfIncrement",
            "Sleep(s)",
            "UseInsConf",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["TotalTimeInSeconds_y"] - x["TotalTimeInSeconds_x"])
            / float(x["TotalTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "update",
        base_version,
        current_version,
    )

    return enriched_table


def compare_stateless_update(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumTasks",
            "Sleep(s)",
            "BatchSize",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["TotalTimeInSeconds_y"] - x["TotalTimeInSeconds_x"])
            / float(x["TotalTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "stateless_update",
        base_version,
        current_version,
    )
    print("compare_stateless_update has created table")
    print(merge_table)
    return enriched_table


def compare_parallel_stateless_update(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumJobs",
            "NumTasks",
            "Sleep(s)",
            "BatchSize",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["AverageTimeInSeconds_y"] - x["AverageTimeInSeconds_x"])
            / float(x["AverageTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "parallel_stateless_update",
        base_version,
        current_version,
    )
    print("compare_parallel_stateless_update has created table")
    print(merge_table)
    return enriched_table


def compare_stateless_create(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumTasks",
            "Sleep(s)",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["TotalTimeInSeconds_y"] - x["TotalTimeInSeconds_x"])
            / float(x["TotalTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "stateless_create",
        base_version,
        current_version,
    )
    print("compare_stateless_create has created table")
    print(merge_table)
    return enriched_table


def compare_stateless_host_limit_1_create(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumTasks",
            "Sleep(s)",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["TotalTimeInSeconds_y"] - x["TotalTimeInSeconds_x"])
            / float(x["TotalTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "stateless_host_limit_1_create",
        base_version,
        current_version,
    )
    print("compare_stateless_host_limit_1_create has created table")
    print(merge_table)
    return enriched_table


def compare_stateless_host_limit_1_update(base_version, current_version, f1, f2):
    dataframe_1, dataframe_2 = to_df(f1), to_df(f2)
    merge_table = pd.merge(
        dataframe_1,
        dataframe_2,
        how="right",
        on=[
            "NumTasks",
            "Sleep(s)",
            "BatchSize",
        ],
    )

    merge_table["Execution Duration Change (%)"] = merge_table.apply(
        lambda x: format(
            (x["TotalTimeInSeconds_y"] - x["TotalTimeInSeconds_x"])
            / float(x["TotalTimeInSeconds_x"])
            * 100,
            ".2f",
        ),
        axis=1,
    )

    enriched_table = styling.enrich_table_layout(
        merge_table,
        "Execution Duration Change (%)",
        "stateless_host_limit_1_update",
        base_version,
        current_version,
    )
    print("compare_stateless_host_limit_1_update has created table")
    print(merge_table)
    return enriched_table


"""
Returns a list of formatted `create`, `get`, and `update` results in HTML
table format.

Args:
    base_version: Peloton perf test base verion
    current_version: current Peloton perf test version
    base_df: pandas.DataFrame, benchmark result on job update
    current_df: pandas.DataFrame, benchmark result on job update

Returns:
    a list of HTML perf test results.
"""


def generate_test_results(
    base_version, current_version, base_df, current_df
):
    operations, results = (
        [
            compare_create,
            compare_get,
            compare_update,
            compare_stateless_create,
            compare_stateless_update,
            compare_parallel_stateless_update,
            compare_stateless_host_limit_1_create,
            compare_stateless_host_limit_1_update,
        ],
        [None] * 8,
    )

    # Aggregates data source with its function operation.
    for i, combo in enumerate(zip(operations, base_df, current_df)):
        func, f1, f2 = combo
        print("Operation " + func.__name__ + " has the following dataframes: ")
        print(f1)
        print(f2)
        try:
            results[i] = func(base_version, current_version, f1, f2)
        except Exception as e:
            results[i] = "<p>Report generation failed: %s</p>" % e
    return results


def send_email(html_msg):
    """
    type html: string
    """
    message = MIMEMultipart("peloton_performance")
    message["Subject"] = "Peloton Performance"
    message["From"] = FROM
    message["To"] = TO

    html = MIMEText(html_msg, "html")

    print("The html generated is: %s", html)
    message.attach(html)

    print("Emailing performance report to " + TO)
    s = smtplib.SMTP("localhost")
    s.sendmail(FROM, [TO], message.as_string())
    s.quit()


def main():
    args = parse_arguments(sys.argv[1:])
    # Get arguments that are passed in.
    base_arg = args.file_1
    current_arg = args.file_2

    # Get version info.
    base_version = base_arg[5:-5]
    current_version = current_arg[5:-8]

    # Get file to read.
    base_results = test_file_path(base_arg)
    current_results = test_file_path(current_arg)

    results = generate_test_results(
        base_version, current_version, base_results, current_results
    )
    create_html = results[0]
    get_html = results[1]
    update_html = results[2]
    stateless_create_html = results[3]
    stateless_update_html = results[4]
    parallel_stateless_update_html = results[5]
    stateless_host_limit_1_create = results[6]
    stateless_host_limit_1_update = results[7]

    commit_info = os.environ.get(COMMIT_INFO) or "N/A"
    build_url = os.environ.get(BUILD_URL) or "N/A"
    msg = HTML_TEMPLATE % (
        commit_info,
        build_url,
        create_html,
        get_html,
        update_html,
        stateless_create_html,
        stateless_update_html,
        parallel_stateless_update_html,
        stateless_host_limit_1_create,
        stateless_host_limit_1_update,
    )
    print(msg)
    if TO:
        send_email(msg)


# Helper function. Converts csv contents to dataframe.
def to_df(csv_file):
    return pd.read_csv(csv_file, "\t", index_col=0)


def test_file_path(input_arg):
    return output_files_list(PERF_DIR + "/", input_arg)


if __name__ == "__main__":
    main()
