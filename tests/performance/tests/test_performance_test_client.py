import unittest

from tests.performance.multi_benchmark import parse_arguments


class MultiBenchmarkTest(unittest.TestCase):
    def test_parser(self):
        parser = parse_arguments(["-i", "CONF_1", "-o", "PERF_2"])
        self.assertEqual(parser.input_file, "CONF_1")
        self.assertEqual(parser.output_file, "PERF_2")
