import json
import unittest

from processor import hurst


class TestHurst(unittest.TestCase):

    def setUp(self):
        self.max_error = .005
        with open("tests/test_hurst_data.json") as test_data_file:
            self.sequences = json.load(test_data_file)
        pass

    def estimatorTest(self, estimator, estimator_name):
        for sequence in self.sequences:
            estimated_hurst_value = estimator(sequence['values'])
            expected_hurst_value = sequence['expected'][estimator_name]
            max_expected_error = expected_hurst_value * self.max_error
            self.assertAlmostEqual(estimated_hurst_value, expected_hurst_value, delta=max_expected_error)

    def testRs(self):
        self.estimatorTest(hurst.rs, 'rs')

    def testWavelet(self):
        self.estimatorTest(hurst.wavelet, 'wavelet')
