import pandas as pd
import subprocess
from .utils import Singleton
from prometheus_api_client import PrometheusConnect, MetricRangeDataFrame
import datetime as dt
import os
import pytz


class PrometheusHelper(metaclass=Singleton):
    def __init__(self):
        self.test_mode = False
        self.prom = None
        self.metrics_list = None
        self.port_forward_ref = None
        self.prometheus_endpoint = "http://localhost:9090"
        self.verbose = False

    def get_metrics_list(self):
        if self.test_mode:
            if self.metrics_list is None:
                metric_path_root = os.path.abspath(f'../../test/test_data/')
                df = pd.read_parquet(f'{metric_path_root}/metrics_list.parquet')
                self.metrics_list = list(set(df['metric_name']))
        else:
            if self.prom is None:
                self.initialize_prom()
        return self.metrics_list

    def get_metric_data_for_range(
            self,
            report_name,
            current_metric,  # this is the metric name and label config
            start_time,
            end_time,
            ) -> pd.DataFrame:
        if isinstance(start_time, str):
            timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
            start_time = dt.datetime.strptime(start_time, timestamp_format)
            start_time = start_time.replace(tzinfo=pytz.utc)
            end_time = dt.datetime.strptime(end_time, timestamp_format)
            end_time = end_time.replace(tzinfo=pytz.utc)
        if self.test_mode:
            metric_path_root = os.path.abspath(f'../test/test_data/{report_name}/')
            try:
                return pd.read_parquet(f'{metric_path_root}/{current_metric}.parquet')
            except Exception as ex:
                return pd.DataFrame()
        else:
            if 'sum' not in current_metric:
                current_df = self.range_metric(current_metric, start_time, end_time, report_name)
            else:
                current_df = self.handle_sum_metric(current_metric, start_time, end_time, report_name)

            return current_df


    def range_metric(self, current_metric, start_time, end_time, report_name) -> pd.DataFrame:
        chunk_size = dt.timedelta(seconds=1)
        if self.prom is None:
            self.initialize_prom()
        try:
            current_df = MetricRangeDataFrame(self.prom.get_metric_range_data(
                current_metric,  # this is the metric name and label config
                start_time=start_time,
                end_time=end_time,
                chunk_size=chunk_size,
            ))
            return current_df
        except Exception as ex:
            if self.verbose:
                import traceback
                print(f'Metric {current_metric} failed to report for report_name {report_name}\n')
                traceback.print_exc()
            return pd.DataFrame()

    def handle_sum_metric(self, current_metric, start_time, end_time, report_name):
        df = self.range_metric(current_metric, start_time, end_time, report_name)
        if df.empty:
            return df
        min_value = df['value'].min(skipna=True)
        df['value'] = df['value'] - min_value
        return df

    def start_port_forward(self):
        prometheus_namespace = os.environ.get('prom_ns', 'monitoring')
        cmd = ["kubectl", "port-forward", "service/prometheus-kube-prometheus-prometheus", "9090:9090", "-n",
               f"{prometheus_namespace}"]
        # Start the command in the background
        self.port_forward_ref = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop_port_forward(self):
        self.port_forward_ref.terminate()  # Sends SIGTERM
        try:
            self.port_forward_ref.wait(timeout=10)  # Wait up to 10 seconds for process to terminate
        except subprocess.TimeoutExpired:
            self.port_forward_ref.kill()  # Force kill if not terminated within timeout
            self.port_forward_ref.wait()  # Wait for the kill to complete
        self.port_forward_ref = None

    def initialize_prom(self):
        self.start_port_forward()
        try:
            # TODO: I may need to add in the code to do port-forwarding commands here!
            self.prom = PrometheusConnect(url=self.prometheus_endpoint, disable_ssl=True)
        except Exception as ex:
            print("Failed to connect to prometheus endpoint!")
            raise ex
        metrics_list = self.prom.all_metrics()
        self.metrics_list = [x for x in metrics_list if 'karpenter' in x]
        print('Count of karpenter metrics is {}'.format(len(self.metrics_list)))

