import sys
from kapenter_evaluate import get_metrics
import os


if __name__ == "__main__":
    csv_dir = os.environ['TEMP_DIR']
    if csv_dir[-1] != '/':
        csv_dir += '/'
    files = os.listdir(csv_dir)
    # Filter the list to include only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        get_metrics(csv_file)
