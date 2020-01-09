from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PapermillOperator(BaseOperator):
    @apply_defaults
    def __init__(self, report, *args, **kwargs):
        super(PapermillOperator, self).__init__(*args, **kwargs)
        self.report = report
        self.nbconvert_task_id = self.task_id.replace('ReportPapermill', 'ReportNBConvert')

    def execute(self, context):
        self.log.critical('papermill')

        from ..worker import run_papermill
        ret = run_papermill(self.report['meta']['notebook'],
                            self.report['meta']['notebook_text'],
                            self.report['meta']['parameters'],
                            self.report['meta']['strip_code'],
                            self.report['meta']['output_path'],
                            self.report['id'])

        task_instance = context['task_instance']
        task_instance.xcom_push(key=self.nbconvert_task_id, value=ret)
