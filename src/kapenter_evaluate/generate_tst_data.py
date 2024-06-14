import awswrangler as aws
import os

import pandas as pd


def fix_arrow_ns_timestamp(df):
    cols_to_fix = [x for x in df.columns if df[x].dtype.name == 'timedelta64[ns]']
    temp_df = df
    for time_col in cols_to_fix:
        df[time_col] = temp_df[[time_col]].astype(int).loc[:, time_col] / (10 ** 9)
    return df


def ensure_directory(dir_path):
    # Convert the path to an absolute path relative to the current working directory
    abs_dir_path = os.path.abspath(dir_path)

    if not os.path.exists(abs_dir_path):
        os.makedirs(abs_dir_path)
        print(f"Directory created: {abs_dir_path}")
    else:
        print(f"Directory already exists: {abs_dir_path}")
    return abs_dir_path


if __name__ == "__main__":
    metrics_list = pd.DataFrame()
    parquet_args = {
        'coerce_timestamps': 'us',
        'allow_truncated_timestamps': True,
    }
    root_path = 's3://containers-ds-eks-simulator-cache2/2024-06-10 07:47:24.505192_01bb91a3-e7a2-47ac-ae51-4099c3a20b8b/'
    mile_stone1 = '1_steady_tps_provisioning_ab_interference_c'
    mile_stone2 = '2_steady_tps_consolidation_ab_test_interference_c'
    target_path = os.path.abspath('../../test/test_data')
    milestone_table = aws.s3.read_parquet(f'{root_path}state_transition_data.parquet')
    milestone1_prom_paths = aws.s3.list_objects(f'{root_path}{mile_stone1}/')
    milestone2_prom_paths = aws.s3.list_objects(f'{root_path}{mile_stone2}/')
    ensure_directory(f'{target_path}/{mile_stone1}/')
    ensure_directory(f'{target_path}/{mile_stone2}/')
    idx = 0
    for prom_path in milestone1_prom_paths:
        temp_df = aws.s3.read_parquet(prom_path)
        metric_name = prom_path.split('/')[-1].replace('_data', '')
        temp_df = fix_arrow_ns_timestamp(temp_df)
        temp_df.to_parquet(f'{target_path}/{mile_stone1}/{metric_name}', **parquet_args)
        metrics_list.loc[idx, 'metric_name'] = metric_name.replace('.parquet', '')
        idx += 1
    for prom_path in milestone2_prom_paths:
        temp_df = aws.s3.read_parquet(prom_path)
        metric_name = prom_path.split('/')[-1].replace('_data', '')
        temp_df = fix_arrow_ns_timestamp(temp_df)
        temp_df.to_parquet(f'{target_path}/{mile_stone2}/{metric_name}', **parquet_args)
        metrics_list.loc[idx, 'metric_name'] = metric_name.replace('.parquet', '')
        idx += 1
    milestone_table[['start_time', 'completed_time', 'end_time']].to_csv(
        f'{target_path}/milestones.csv'
    )
    metrics_list.drop_duplicates(inplace=True)
    metrics_list.to_parquet(
        f'{target_path}/metrics_list.parquet'
    )
    print('here')