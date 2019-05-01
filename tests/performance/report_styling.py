#!/usr/bin/env python
from bs4 import BeautifulSoup
import pandas as pd

##########################################################
# Style performance report table's column layout.
##########################################################

TABLE_ID = [("border", 1), ("class", "dataframe wide")]

CREATE_TABLE_HEADER_TEMPLATE = """
    <tr style="text-align: right;">
      <th class="blank level0" rowspan="2"></th>
      <th class="col_heading level0 col0" rowspan="2">Cores</th>
      <th class="col_heading level0 col1" rowspan="2">TaskNum</th>
      <th class="col_heading level0 col2" rowspan="2">Sleep(s)</th>
      <th class="col_heading level0 col3" rowspan="2">Use InstanceCfg</th>
      <th class="col_heading level0 col4 beforeCommit" colspan="2">
        Baseline {}
      </th>
      <th class="col_heading level0 col5 afterCommit" colspan="2">
        Current {}
      </th>
      <th class="col_heading level0 col6 results" rowspan="2">
        Execution Duration Change (%)
      </th>
    </tr>
    <tr>
      <th class="col_heading level1 col1">Start Time(s)</th>
      <th class="col_heading level1 col2">Duration (in sec)</th>
      <th class="col_heading level1 col4">Start Time(s)</th>
      <th class="col_heading level1 col5">Duration (in sec)</th>
    </tr>
"""


GET_TABLE_HEADER_TEMPLATE = """
    <tr style="text-align: right;">
      <th class="blank level0" rowspan="3"></th>
      <th class="col_heading level0 col0" rowspan="3">TaskNum</th>
      <th class="col_heading level0 col1" rowspan="3">Sleep(s)</th>
      <th class="col_heading level0 col2" rowspan="3">UseInsConf</th>
      <th class="col_heading level0 col3 beforeCommit" colspan="4">
        Baseline {}
      </th>
      <th class="col_heading level0 col3 afterCommit" colspan="4">
        Current {}
      </th>
      <th class="col_heading level0 col6 results" rowspan="3">
        Get Counts Change (%)
      </th>
    </tr>
    <tr>
      <th class="col_heading level1 col1 beforeGranular1A" colspan="2">
        Create
      </th>
      <th class="col_heading level1 col2 beforeGranular1A" colspan="2">Get</th>
      <th class="col_heading level1 col3 afterGranular1A" colspan="2">
        Create
      </th>
      <th class="col_heading level1 col4 afterGranular1A" colspan="2">Get</th>
    </tr>
    <tr>
      <th class="col_heading level2 col1">Counts (Success)</th>
      <th class="col_heading level2 col2">Counts (Failure)</th>
      <th class="col_heading level2 col3">Counts (Success)</th>
      <th class="col_heading level2 col4">Counts (Failure)</th>
      <th class="col_heading level2 col5">Counts (Success)</th>
      <th class="col_heading level2 col6">Counts (Failure)</th>
      <th class="col_heading level2 col7">Counts (Success)</th>
      <th class="col_heading level2 col8">Counts (Failure)</th>
    </tr>
    """

UPDATE_TABLE_HEADER_TEMPLATE = """
    <tr style="text-align: right;">
        <th class="blank level0" rowspan="2"></th>
        <th class="col_heading level0 col0" rowspan="2">
          Start Task Counts
        </th>
        <th class="col_heading level0 col1" rowspan="2">
          Task Inc each Iteration
        </th>
        <th class="col_heading level0 col2" rowspan="2">Inc Counts</th>
        <th class="col_heading level0 col3" rowspan="2">Sleep(s)</th>
        <th class="col_heading level0 col4" rowspan="2">Use InstanceCfg</th>
        <th class="col_heading level0 col5 beforeCommit">Baseline {}</th>
        <th class="col_heading level0 col6 afterCommit">Current {}</th>
        <th class="col_heading level0 col7 results" rowspan="2">
          Execution Duration Change (%)
        </th>
    </tr>

    <tr>
        <th class="col_heading level1 col1 beforeGranular1B">
          Duration (in sec)
        </th>
        <th class="col_heading level1 col2 afterGranular1B">
          Duration (in sec)
        </th>
    </tr>
"""

HEADER = {
    "create": CREATE_TABLE_HEADER_TEMPLATE,
    "get": GET_TABLE_HEADER_TEMPLATE,
    "update": UPDATE_TABLE_HEADER_TEMPLATE,
}

CSS_STYLE = """
    h2 {
        text-align: center;
        font-family: Helvetica, Arial, sans-serif;
    }
    table {
        margin-left: auto;
        margin-right: auto;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
    }
    th, td {
        padding: 5px;
        text-align: center;
        font-family: Helvetica, Arial, sans-serif;
        font-size: 90%;
    }
    table tbody tr:hover {
        background-color: #dddddd;
    }
    .wide {
        width: 90%;
    }

    .beforeCommit {
      background-color: #9fad9f;
    }

    .beforeGranular1A {
      background-color: #e8f9e8;
    }

    .afterCommit {
      background-color: #b1c4cc;
    }

    .afterGranular1A {
      background-color: #e1f1f7;
    }

    .results {
      background-color: #F0FFF0;
    }
"""

"""
Update the report table with improved layout.

Args:
  colored_df: dataframe that has been applied coloring
  df_type: usage of dataframe, either it's 'create', 'get', or 'update'.
  base_version: Peloton perf test base verion
  current_version: current Peloton perf test version

Returns:
  Performance report in html format.
"""


def update_column_groupings(
    colored_df, df_type, base_version, current_version
):
    df_id = TABLE_ID
    header = _fill_version_info(HEADER[df_type], base_version, current_version)
    css_style = CSS_STYLE
    soup = BeautifulSoup(colored_df)

    # update df_id in html
    for name, val in df_id:
        soup.table[name] = val

    # update header
    html_header = BeautifulSoup(header)
    soup.tr.replace_with(html_header)

    # update css style
    css_tag = soup.new_tag("style")
    css_tag.string = css_style
    soup.table.insert_before(css_tag)

    return soup


"""
Apply background color if the performance results differ by more than 0,1
between the two versions.

If the new version improves more than 10%, the result is colored green.
If the new version has more than 10% performance regression, the result is
colored red. For the rest of the results, no background color is applied.

Args:
  df: pandas.DataFrame
  col_name: results from which column to look at

Return:
  rendered HTML object
"""


def apply_bgcolor(df, df_type, col_name):
    def _results_style(row, df_type, col_name):
        col_val = float(row[col_name])
        if df_type == "get":
            col_val = col_val * (-1)

        if col_val > 10:
            return pd.Series("background-color: #ea2323", row.index)  # red
        else:
            return pd.Series("", row.index)

    df_style = df.style.apply(
        _results_style,
        df_type=df_type,
        col_name=col_name,
        axis=1,
        subset=[col_name],
    )
    rendered = df_style.render()

    return rendered


"""
Modify automatically generated DataFrame with better visual styling and a new
hierarchical table header, which organize results into "Base (<version>)"
and "Current (<version>)".
"""


def enrich_table_layout(df, col_name, df_type, base_version, current_version):
    colored_dataframe = apply_bgcolor(df, df_type, col_name)
    style_results = update_column_groupings(
        colored_dataframe, df_type, base_version, current_version
    )
    return style_results


"""
Add base version and current version information to the table header template.
"""


def _fill_version_info(html_template, base_version, current_version):
    return html_template.format(base_version, current_version)
