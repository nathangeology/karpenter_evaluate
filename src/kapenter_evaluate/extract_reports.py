import pandas as pd
import pkg_resources
from .prometheus_helper import PrometheusHelper
from prometheus_api_client import PrometheusConnect, MetricRangeDataFrame
import datetime as dt
import pytz
from .utils import CachingAgent
from .compute_run_kpis import extract_raw_milestone_kpis
cum_cols = ['pending_pod_secs', 'nodes_terminated', 'nodes_created', 'provisioning_sched_simulation_sum',
                    'disruption_sched_simulation_sum', 'total_scheduling_sim_time', 'pod_scheduling_time',
                    'disruption_eval_duration_sum_drift', 'disruption_eval_duration_sum_emptiness',
                              'disruption_eval_duration_sum_expiration', 'disruption_eval_duration_sum_multi',
                              'disruption_eval_duration_sum_empty', 'disruption_eval_duration_sum_single',
                    'total_disruption_evaluation_duration_sum',
                    ]


def get_metrics(timestamp_csv: str, run_name: str, verbose: bool, prometheus_endpoint="http://localhost:9090") -> pd.DataFrame:
    """Provide a local filepath to a csv file that contains the milestone start and stop times"""
    output = pd.DataFrame()
    current_time = dt.datetime.now(tz=pytz.utc)
    metrics_list = PrometheusHelper().get_metrics_list()
    if verbose:
        PrometheusHelper().verbose = verbose
    try:
        milestones_df = pd.read_csv(timestamp_csv, index_col=0)
    except Exception as ex:
        if verbose:
            print("loading milestone's timestamps csv failed for some reason.")
        raise ex
    milestone_count = 0
    report_list = []
    for idx, row in milestones_df.iterrows():
        if idx == 'end_of_actions':
            continue
        write_reports_for_timestamps(
            row['Start'],
            row['End'],
            metrics_list,
            report_name=idx
        )
        try:
            output = extract_raw_milestone_kpis(idx, output, milestones_df, verbose)
        except Exception as ex:
            if idx == 'AfterEach':
                """After each seems to be too fast to get some 
                metrics most of the time, so we should probably skip it when it errors."""
                continue
        milestone_count += 1
        output['test_name'] = run_name
        report_list.append(idx)

    # TODO: It may be nice to collect other info about the run (e.g. karpenter version, commit tag, etc) for longer term reports here
    output['report_timestamp'] = current_time
    # TODO: Setup long term collection of the test reports to a central location for analysis
    if verbose:
        print(output.to_markdown())
    CachingAgent().cache_dataframe(f'final_report-{run_name}.parquet', output)
    return output


def write_reports_for_timestamps(start_ts, end_ts, metrics_list, report_name):
    for current_metric in metrics_list:

        current_df = PrometheusHelper().get_metric_data_for_range(
            report_name=report_name,
            start_time=start_ts,
            end_time=end_ts,
            current_metric=current_metric
        )
        if current_df.empty:
            continue
        current_df['ts'] = current_df.index
        CachingAgent().cache_dataframe(f'/{report_name}/{current_metric}.parquet', current_df)
