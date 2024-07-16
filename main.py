import sys

import pandas as pd
from kapenter_evaluate import get_metrics, PrometheusHelper
import os
import argparse


if __name__ == "__main__":
    verbose = False
    parser = argparse.ArgumentParser(description='Example script with --verbose flag.')
    # Add the --verbose flag
    parser.add_argument('--verbose', action='store_true', help='Increase output verbosity')
    args = parser.parse_args()
    # Check if --verbose flag is set
    if args.verbose:
        print("Verbose mode is ON")
        verbose = True
        # Perform additional logging or actions for verbose mode

    reports = []
    csv_dir = os.environ['OUTPUT_DIR']
    print(f'The csv dir is {csv_dir}\n')
    if '\n' in csv_dir:
        csv_dir = csv_dir.split('\n')[0]
    if csv_dir[-1] != '/':
        csv_dir += '/'
    files = os.listdir(csv_dir)
    # Filter the list to include only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        comps = csv_file.split('-')
        test_name = comps[-1].split('.')[0]
        if csv_dir not in csv_file:
            csv_file = csv_dir + csv_file
        report = get_metrics(csv_file, test_name, verbose)
        reports.append(report)
    total_report = pd.concat(reports)
    print('KPIs for all tests: \n')
    print(total_report.to_markdown())
    PrometheusHelper().stop_port_forward()
