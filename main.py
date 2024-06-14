import sys
from kapenter_evaluate import get_metrics
import os


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <filename>")
        sys.exit(1)

    csv_dir = sys.argv[1]
    files = os.listdir(csv_dir)
    # Filter the list to include only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        get_metrics(csv_file)
