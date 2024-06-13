from .utils import CachingAgent


def extract_raw_milestone_kpis(report_name, output, milestones_df):
    pending_secs_df = CachingAgent().get_dataframe(
        f'{report_name}/karpenter_pods_startup_time_seconds_sum')
    output.loc[report_name, 'pending_pod_secs'] = pending_secs_df['value'].max()
    max_queue_df = CachingAgent().get_dataframe(
        f'{report_name}/karpenter_provisioner_scheduling_queue_depth')
    output.loc[report_name, 'total_end_to_end_time(sec)'] = (
            milestones_df.at[report_name, 'completed_time'] - milestones_df.at[
        report_name, 'start_time']).seconds
    output.loc[report_name, 'max_provisioning_queue'] = max_queue_df.loc[
        max_queue_df['controller'] == 'provisioner', 'value'].max()
    output.loc[report_name, 'max_disruption_queue'] = max_queue_df.loc[
        max_queue_df['controller'] == 'disruption', 'value'].max()
    # NODES TERMINATED
    try:
        nodes_terminated_df = CachingAgent().get_dataframe(
            f'{report_name}/karpenter_nodes_terminated')
        node_term_count = nodes_terminated_df['value'].max()
    except Exception as ex:
        node_term_count = 0
    output.loc[report_name, 'nodes_terminated'] = node_term_count
    # NODE PROVISIONED
    nodes_created_df = CachingAgent().get_dataframe(f'{report_name}/karpenter_nodes_created')
    output.loc[report_name, 'nodes_created'] = nodes_created_df['value'].max()
    # SIM TIME SUM (TOTAL AND BROKEN OUT)
    shed_sim_df = CachingAgent().get_dataframe(
        f'{report_name}/karpenter_provisioner_scheduling_simulation_duration_seconds_sum')
    output.loc[report_name, 'provisioning_sched_simulation_sum'] = shed_sim_df.loc[
        shed_sim_df['controller'] == 'provisioner', 'value'].max()
    output.loc[report_name, 'disruption_sched_simulation_sum'] = shed_sim_df.loc[
        shed_sim_df['controller'] == 'disruption', 'value'].max()
    output.loc[report_name, 'total_scheduling_sim_time'] = output.loc[
                                                                       report_name, 'provisioning_sched_simulation_sum'] + \
                                                                   output.loc[
                                                                       report_name, 'disruption_sched_simulation_sum']
    # Scheduling Time
    shed_df = CachingAgent().get_dataframe(
        f'{report_name}/karpenter_provisioner_scheduling_duration_seconds_sum')
    output.loc[report_name, 'pod_scheduling_time'] = shed_df['value'].max()
    # DISRUPTION TIME SUM (SAME)
    disrupt_pending_secs_df = CachingAgent().get_dataframe(
        f'{report_name}/karpenter_disruption_evaluation_duration_seconds_sum')
    disruption_outputs = ['disruption_eval_duration_sum_drift', 'disruption_eval_duration_sum_emptiness',
                          'disruption_eval_duration_sum_expiration', 'disruption_eval_duration_sum_multi',
                          'disruption_eval_duration_sum_empty', 'disruption_eval_duration_sum_single',

                          ]
    output.loc[report_name, 'disruption_eval_duration_sum_drift'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['method'] == 'drift', 'value'].max()
    output.loc[report_name, 'disruption_eval_duration_sum_emptiness'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['method'] == 'emptiness', 'value'].max()
    output.loc[report_name, 'disruption_eval_duration_sum_expiration'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['method'] == 'expiration', 'value'].max()
    output.loc[report_name, 'disruption_eval_duration_sum_multi'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['consolidation_type'] == 'multi', 'value'].max()
    output.loc[report_name, 'disruption_eval_duration_sum_empty'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['consolidation_type'] == 'empty', 'value'].max()
    output.loc[report_name, 'disruption_eval_duration_sum_single'] = disrupt_pending_secs_df.loc[
        disrupt_pending_secs_df['consolidation_type'] == 'single', 'value'].max()
    output.loc[report_name, 'total_disruption_evaluation_duration_sum'] = (
        sum([output.at[report_name, x] for x in disruption_outputs]))
    return output
    
def correct_sum_metrics_kpis():
    pass

def report_kpis():
    pass
