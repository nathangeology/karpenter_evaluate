import sys
from kapenter_evaluate import get_metrics, PrometheusHelper
import os


if __name__ == "__main__":
    csv_dir = os.environ['TEMP_DIR']
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
        get_metrics(csv_file, test_name)
    PrometheusHelper().stop_port_forward()
