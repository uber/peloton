import unittest
from StringIO import StringIO
from tests.performance.perf_compare import (
    parse_arguments,
    perf_compare
)
import pandas as pd

DF_1 = """
\tCores\tTaskNum\tSleep(s)\tUseInsConf\tVersion\tStart(s)\tExec(s)
0\t1000\t5000\t10\tTrue\t0.6.9-86-g7135a0e\t3.97567\t24.73733
"""

DF_2 = """
\tCores\tTaskNum\tSleep(s)\tUseInsConf\tVersion\tStart(s)\tExec(s)
0\t1000\t5000\t10\tTrue\t0.6.11-5-gb4a1c68\t4.31115\t20.98786
"""


class PerfCompareTest(unittest.TestCase):
    def test_parser(self):
        parser = parse_arguments(['-f1', 'PERF_1', '-f2', 'PERF_2'])
        self.assertEqual(parser.file_1, 'PERF_1')
        self.assertEqual(parser.file_2, 'PERF_2')

    def test_perf_compare(self):
        df1 = pd.read_csv(StringIO(DF_1), '\t', index_col=0)
        df2 = pd.read_csv(StringIO(DF_2), '\t', index_col=0)

        df_out = perf_compare(df1, df2)
        self.assertEqual(df_out.iloc[0]['Perf Change'], '-0.1516')
