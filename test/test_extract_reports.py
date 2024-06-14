from kapenter_evaluate import get_metrics, PrometheusHelper
import os


class TestReportGeneration():
    def test_get_metrics(self):
        csv_path = os.path.abspath('./test_data/milestones.csv')
        PrometheusHelper().test_mode = True
        get_metrics(timestamp_csv=csv_path)
        print('here')

