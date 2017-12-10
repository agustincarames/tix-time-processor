import argparse


def parse_args(raw_args=None):
    parser = argparse.ArgumentParser(description='Script to shape the report files from the tix-time-condenser into '
                                                 'the batches. This is to imitate the way the tix-time-processor takes '
                                                 'the files and computes them by separating them into different '
                                                 'directories. The idea behind this is to use the files for '
                                                 'exploratory analysis.')
    parser.add_argument('source_directory',
                        help='The path to the directory where the reports are.')
    parser.add_argument('--output', '-o', action='store', default='batch-test-report.tar.gz', type=str,
                        help='The name of the output file. By default "batch-test-report.tar.gz".')
    args = parser.parse_args(raw_args)
    return args
