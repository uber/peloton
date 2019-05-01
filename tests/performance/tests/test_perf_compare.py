import unittest
from StringIO import StringIO
from tests.performance.perf_compare import (
    parse_arguments,
    compare_create,
    compare_get,
    compare_update,
)
import pandas as pd

CREATE_DF_1 = """
\tCores\tTaskNum\tSleep(s)\tUseInsConf\tStart(s)\tExec(s)
0\t1000\t5000\t10\tTrue\t3.97567\t24.73733
"""

CREATE_DF_2 = """
\tCores\tTaskNum\tSleep(s)\tUseInsConf\tStart(s)\tExec(s)
0\t1000\t5000\t10\tTrue\t4.31115\t20.98786
"""

GET_DF_1 = """
\tTaskNum\tSleep(s)\tUseInsConf\tCreates\tCreateFails\tGets\tGetFails
0\t5000\t10\t70\t3\t73\t0
"""

GET_DF_2 = """
\tTaskNum\tSleep(s)\tUseInsConf\tCreates\tCreateFails\tGets\tGetFails
0\t5000\t10\t70\t3\t60\t13
"""

UPDATE_DF_1 = """
\tNumStartTasks\tTaskIncrementEachTime\tNumOfIncrement\tSleep(s)\tUseInsConf\tTotalTimeInSeconds
5\t1\t1\t5000\t10\t850
"""

UPDATE_DF_2 = """
\tNumStartTasks\tTaskIncrementEachTime\tNumOfIncrement\tSleep(s)\tUseInsConf\tTotalTimeInSeconds
5\t1\t1\t5000\t10\t950
"""


class PerfCompareTest(unittest.TestCase):
    def test_parser(self):
        parser = parse_arguments(["-f1", "PERF_1", "-f2", "PERF_2"])
        self.assertEqual(parser.file_1, "PERF_1")
        self.assertEqual(parser.file_2, "PERF_2")

    def test_compare_create(self):
        df1 = pd.read_csv(StringIO(CREATE_DF_1), "\t", index_col=0)
        df2 = pd.read_csv(StringIO(CREATE_DF_2), "\t", index_col=0)

        df_out = compare_create(df1, df2)
        self.assertEqual(df_out.iloc[0]["Perf Change"], "-0.1516")

    def test_compare_get(self):
        df1 = pd.read_csv(StringIO(GET_DF_1), "\t", index_col=0)
        df2 = pd.read_csv(StringIO(GET_DF_2), "\t", index_col=0)
        df_out = compare_get(df1, df2)

        shared_fields = ["TaskNum", "Sleep(s)", "UseInsConf"]
        for field in shared_fields:
            self.assertEqual(df_out.iloc[0][field], df_out.iloc[1][field])

    def test_compare_update(self):
        df1 = pd.read_csv(StringIO(UPDATE_DF_1), "\t", index_col=0)
        df2 = pd.read_csv(StringIO(UPDATE_DF_2), "\t", index_col=0)
        df_out = compare_update(df1, df2)

        self.assertEqual(df_out.iloc[0]["Time Diff"], "100")
