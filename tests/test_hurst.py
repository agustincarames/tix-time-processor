import unittest

from tdl import noise

from processor import hurst


class TestHurst(unittest.TestCase):

    def setUp(self):
        hurst_values = [0.5, 0.6, 0.65, 0.68, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
        sequence_lengths = [2 ** 10, 2 ** 11, 2 ** 12]
        self.sequences = []
        for hurst_value in hurst_values:
            noise_generator = noise.Noise(mode='TURBULENCE', hurst=hurst_value, dimensions=2)
            for sequence_length in sequence_lengths:
                self.sequences.append({
                    'hurst_value': hurst_value,
                    'noise': [noise_generator.get_point(.5, point) for point in range(sequence_length)]
                })
        pass

    def testRs(self):
        for sequence in self.sequences:
            hurst_value = hurst.rs(sequence['noise'])
            expected_hurst_value = sequence['hurst_value']
            max_error = expected_hurst_value * .2
            self.assertAlmostEqual(hurst_value, sequence['hurst_value'], delta=max_error)

    def testWavelet(self):
        for sequence in self.sequences:
            hurst_value = hurst.wavelet(sequence['noise'])
            expected_hurst_value = sequence['hurst_value']
            max_error = expected_hurst_value * .2
            self.assertAlmostEqual(hurst_value, sequence['hurst_value'], delta=max_error)
