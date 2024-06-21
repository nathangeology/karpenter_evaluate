from kapenter_evaluate import get_metrics, PrometheusHelper
import sys
import os


def test_eval_live_local_cluster():
    path = '/tmp/117510211/'
    files = os.listdir(path)
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        if path not in csv_files:
            csv_file = path + csv_file
        comps = csv_file.split('-')
        test_name = comps[-1].split('.')[0]
        get_metrics(csv_file, test_name)
    PrometheusHelper().stop_port_forward()
    print('done')