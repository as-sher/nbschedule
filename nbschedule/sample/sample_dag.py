# This is the jinja2 templatized DAG that generates
# the job operator. It has the following form as an
# example:
#
#    Job -> Report -> Papermill -> NBConvert ->      ReportPost -\
#      \--> Report -> Papermill -> Voila -> Dokku -> ReportPost --\
#       \-> Report -> Papermill -> NBConvert ->      ReportPost ----> Job Cleanup
#

import json
from base64 import b64decode

# Base Job Operators
from nbschedule.operator.common import JobOperator, JobCleanupOperator

# Base Report Operators


# Convert Operators
from nbschedule.operator import PapermillOperator

# Publish operators


from airflow import DAG
from datetime import timedelta, datetime


###################################
# Default arguments for operators #
###################################

# DAG args: https://airflow.incubator.apache.org/code.html?highlight=dag#airflow.models.DAG
dag_args = {
    'description': u'',
    'schedule_interval': '*/30 * * * *',
    'start_date': datetime.strptime('01/09/2020 06:38:00', '%m/%d/%Y %H:%M:%S'),
    'end_date': None,
    'full_filepath': None,
    'template_searchpath': None,
    'user_defined_macros': None,
    'user_defined_filters': None,
    'concurrency': 16,
    'max_active_runs': 16,
    'dagrun_timeout': None,
    'sla_miss_callback': None,
    'default_view': u'graph',
    'orientation': 'LR',
    'catchup': False,
    'on_success_callback': None,
    'on_failure_callback': None,
    # 'params': None,
}

# Operator args: https://airflow.incubator.apache.org/code.html#baseoperator
default_operator_args = {
    'owner': 'admin',
    'email_on_retry': False,
    'email_on_failure': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': False,
    'max_retry_delay': None,
    'start_date': None,
    'end_date': None,
    'schedule_interval': None,
    'depends_on_past': False,
    'wait_for_downstream': False,
    # 'params': None,
    'default_args': None,
    'adhoc': False,
    'priority_weight': 1,
    'weight_rule': u'downstream',
    'queue': 'default',
    'pool': 'default_pool',
    'sla': None,
    'execution_timeout': None,
    'on_failure_callback': None,
    'on_success_callback': None,
    'on_retry_callback': None,
    'trigger_rule': u'all_success',
    'resources': None,
    'run_as_user': None,
    'task_concurrency': None,
    'executor_config': None,
    'inlets': None,
    'outlets': None,
}

###################################
# Inline job and reports as b64 json  #
###################################
job_json = json.loads(b64decode(b'eyJuYW1lIjogIm1zZ3NwcmludDMiLCAiaWQiOiAiSm9iLTYiLCAibWV0YSI6IHsibm90ZWJvb2siOiAibXNnc3ByaW50IiwgIm5vdGVib29rX3RleHQiOiAie1xuIFwiY2VsbHNcIjogW1xuICB7XG4gICBcImNlbGxfdHlwZVwiOiBcImNvZGVcIixcbiAgIFwiZXhlY3V0aW9uX2NvdW50XCI6IG51bGwsXG4gICBcIm1ldGFkYXRhXCI6IHtcbiAgICBcInRhZ3NcIjogW1xuICAgICBcInBhcmFtZXRlcnNcIlxuICAgIF1cbiAgIH0sXG4gICBcIm91dHB1dHNcIjogW10sXG4gICBcInNvdXJjZVwiOiBbXG4gICAgXCJtc2dzPVxcXCJcXFwiXCJcbiAgIF1cbiAgfSxcbiAge1xuICAgXCJjZWxsX3R5cGVcIjogXCJjb2RlXCIsXG4gICBcImV4ZWN1dGlvbl9jb3VudFwiOiBudWxsLFxuICAgXCJtZXRhZGF0YVwiOiB7fSxcbiAgIFwib3V0cHV0c1wiOiBbXSxcbiAgIFwic291cmNlXCI6IFtcbiAgICBcInByaW50KG1zZ3MpXCJcbiAgIF1cbiAgfSxcbiAge1xuICAgXCJjZWxsX3R5cGVcIjogXCJjb2RlXCIsXG4gICBcImV4ZWN1dGlvbl9jb3VudFwiOiBudWxsLFxuICAgXCJtZXRhZGF0YVwiOiB7fSxcbiAgIFwib3V0cHV0c1wiOiBbXSxcbiAgIFwic291cmNlXCI6IFtcbiAgICBcImltcG9ydCBwYW5kYXNcIlxuICAgXVxuICB9XG4gXSxcbiBcIm1ldGFkYXRhXCI6IHtcbiAgXCJjZWxsdG9vbGJhclwiOiBcIlRhZ3NcIixcbiAgXCJrZXJuZWxzcGVjXCI6IHtcbiAgIFwiZGlzcGxheV9uYW1lXCI6IFwiUHl0aG9uIDNcIixcbiAgIFwibGFuZ3VhZ2VcIjogXCJweXRob25cIixcbiAgIFwibmFtZVwiOiBcInB5dGhvbjNcIlxuICB9LFxuICBcImxhbmd1YWdlX2luZm9cIjoge1xuICAgXCJjb2RlbWlycm9yX21vZGVcIjoge1xuICAgIFwibmFtZVwiOiBcImlweXRob25cIixcbiAgICBcInZlcnNpb25cIjogM1xuICAgfSxcbiAgIFwiZmlsZV9leHRlbnNpb25cIjogXCIucHlcIixcbiAgIFwibWltZXR5cGVcIjogXCJ0ZXh0L3gtcHl0aG9uXCIsXG4gICBcIm5hbWVcIjogXCJweXRob25cIixcbiAgIFwibmJjb252ZXJ0X2V4cG9ydGVyXCI6IFwicHl0aG9uXCIsXG4gICBcInB5Z21lbnRzX2xleGVyXCI6IFwiaXB5dGhvbjNcIixcbiAgIFwidmVyc2lvblwiOiBcIjMuNy4wXCJcbiAgfSxcbiAgXCJwYXBlcm1pbGxcIjoge1xuICAgXCJkdXJhdGlvblwiOiA1MS44NjQ5OTQsXG4gICBcImVuZF90aW1lXCI6IFwiMjAxOS0xMi0xOVQwNjoyNTo1MS43Mjg2MjhcIixcbiAgIFwiZW52aXJvbm1lbnRfdmFyaWFibGVzXCI6IHt9LFxuICAgXCJleGNlcHRpb25cIjogbnVsbCxcbiAgIFwiaW5wdXRfcGF0aFwiOiBcIlVudGl0bGVkLmlweW5iXCIsXG4gICBcIm91dHB1dF9wYXRoXCI6IFwiT3V0cHV0LmlweW5iXCIsXG4gICBcInBhcmFtZXRlcnNcIjoge30sXG4gICBcInN0YXJ0X3RpbWVcIjogXCIyMDE5LTEyLTE5VDA2OjI0OjU5Ljg2MzYzNFwiLFxuICAgXCJ2ZXJzaW9uXCI6IFwiMS4yLjFcIlxuICB9XG4gfSxcbiBcIm5iZm9ybWF0XCI6IDQsXG4gXCJuYmZvcm1hdF9taW5vclwiOiA0XG59IiwgImludGVydmFsIjogIm1pbnV0ZWx5IiwgImxldmVsIjogInByb2R1Y3Rpb24iLCAicmVwb3J0cyI6IDIsICJjcmVhdGVkIjogIjAxLzA5LzIwMjAgMTI6MTE6NTQiLCAibW9kaWZpZWQiOiAiMDEvMDkvMjAyMCAxMjoxMTo1NCJ9fQ=='))
reports_json = json.loads(b64decode(b'W3snbmFtZSc6ICdteXJlcG9ydDEnLCAnaWQnOiAnb3V0cHV0LWZpbGUtbmFtZScsICdtZXRhJzogeydub3RlYm9vayc6ICdteW5vdGVib29rJywgJ291dHB1dF9wYXRoJzogJy9Vc2Vycy90a21hZGt2L0FzaHV0b3NoL3NvbWVwYWNrYWdlL25ic2NoZWR1bGUvc2FtcGxlJywgJ3BhcmFtZXRlcnMnOiAneyJtc2dzIjogIkhlbGxvIn0nLCAndHlwZSc6ICdjb252ZXJ0JywgJ291dHB1dCc6ICdub3RlYm9vaycsICdzdHJpcF9jb2RlJzogVHJ1ZSwgJ3J1bic6ICdub3QgcnVuJ319LCB7J2lkJzogJ215UmVwb3J0LTI2JywgJ21ldGEnOiB7J25vdGVib29rJzogJ215bm90ZWJvb2snLCAncGFyYW1ldGVycyc6ICd7Im1zZ3MiOiAiR29vZCBNb3JuaW5nIn0nLCAndHlwZSc6ICdjb252ZXJ0JywgJ291dHB1dCc6ICdub3RlYm9vaycsICdzdHJpcF9jb2RlJzogVHJ1ZSwgJ3J1bic6ICdub3QgcnVuJ319XQ=='))

###################################
# Create dag from job and reports #
###################################

# The top level dag, representing a Job run on a Notebook
dag = DAG('DAG-' + str(job_json['id']), default_args=default_operator_args, **dag_args)

# The Job operator, used for bundling groups of reports,
# setting up env/image
job = JobOperator(job=job_json, task_id='Job-{}'.format(job_json['id']), dag=dag)

# The cleanup operator, run after all reports are finished
cleanup = JobCleanupOperator(job=job_json, task_id='JobCleanup-{}'.format(job_json['id']), dag=dag)

for rep in reports_json:
    # copy over notebook text (only store 1 copy in the job json)
    rep['meta']['notebook_text'] = job_json['meta']['notebook_text']


    # Papermill operator, performs the report creation
    # using papermill and the report's individual
    # parameters and configuration
    pp = PapermillOperator(report=rep,
                           task_id='ReportPapermill-{}'.format(rep['id']),
                           dag=dag)
    job.set_downstream(pp)
    pp.set_downstream(cleanup)
