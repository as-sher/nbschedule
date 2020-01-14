import time
from pprint import pprint
import json
from base64 import b64decode
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime



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



job_json = json.loads(b64decode(b'eyJuYW1lIjogIm1zZ3NwcmludDMiLCAiaWQiOiAiSm9iLTYiLCAibWV0YSI6IHsibm90ZWJvb2siOiAibXNnc3ByaW50IiwgIm5vdGVib29rX3RleHQiOiAie1xuIFwiY2VsbHNcIjogW1xuICB7XG4gICBcImNlbGxfdHlwZVwiOiBcImNvZGVcIixcbiAgIFwiZXhlY3V0aW9uX2NvdW50XCI6IG51bGwsXG4gICBcIm1ldGFkYXRhXCI6IHtcbiAgICBcInRhZ3NcIjogW1xuICAgICBcInBhcmFtZXRlcnNcIlxuICAgIF1cbiAgIH0sXG4gICBcIm91dHB1dHNcIjogW10sXG4gICBcInNvdXJjZVwiOiBbXG4gICAgXCJtc2dzPVxcXCJcXFwiXCJcbiAgIF1cbiAgfSxcbiAge1xuICAgXCJjZWxsX3R5cGVcIjogXCJjb2RlXCIsXG4gICBcImV4ZWN1dGlvbl9jb3VudFwiOiBudWxsLFxuICAgXCJtZXRhZGF0YVwiOiB7fSxcbiAgIFwib3V0cHV0c1wiOiBbXSxcbiAgIFwic291cmNlXCI6IFtcbiAgICBcInByaW50KG1zZ3MpXCJcbiAgIF1cbiAgfSxcbiAge1xuICAgXCJjZWxsX3R5cGVcIjogXCJjb2RlXCIsXG4gICBcImV4ZWN1dGlvbl9jb3VudFwiOiBudWxsLFxuICAgXCJtZXRhZGF0YVwiOiB7fSxcbiAgIFwib3V0cHV0c1wiOiBbXSxcbiAgIFwic291cmNlXCI6IFtcbiAgICBcImltcG9ydCBwYW5kYXNcIlxuICAgXVxuICB9XG4gXSxcbiBcIm1ldGFkYXRhXCI6IHtcbiAgXCJjZWxsdG9vbGJhclwiOiBcIlRhZ3NcIixcbiAgXCJrZXJuZWxzcGVjXCI6IHtcbiAgIFwiZGlzcGxheV9uYW1lXCI6IFwiUHl0aG9uIDNcIixcbiAgIFwibGFuZ3VhZ2VcIjogXCJweXRob25cIixcbiAgIFwibmFtZVwiOiBcInB5dGhvbjNcIlxuICB9LFxuICBcImxhbmd1YWdlX2luZm9cIjoge1xuICAgXCJjb2RlbWlycm9yX21vZGVcIjoge1xuICAgIFwibmFtZVwiOiBcImlweXRob25cIixcbiAgICBcInZlcnNpb25cIjogM1xuICAgfSxcbiAgIFwiZmlsZV9leHRlbnNpb25cIjogXCIucHlcIixcbiAgIFwibWltZXR5cGVcIjogXCJ0ZXh0L3gtcHl0aG9uXCIsXG4gICBcIm5hbWVcIjogXCJweXRob25cIixcbiAgIFwibmJjb252ZXJ0X2V4cG9ydGVyXCI6IFwicHl0aG9uXCIsXG4gICBcInB5Z21lbnRzX2xleGVyXCI6IFwiaXB5dGhvbjNcIixcbiAgIFwidmVyc2lvblwiOiBcIjMuNy4wXCJcbiAgfSxcbiAgXCJwYXBlcm1pbGxcIjoge1xuICAgXCJkdXJhdGlvblwiOiA1MS44NjQ5OTQsXG4gICBcImVuZF90aW1lXCI6IFwiMjAxOS0xMi0xOVQwNjoyNTo1MS43Mjg2MjhcIixcbiAgIFwiZW52aXJvbm1lbnRfdmFyaWFibGVzXCI6IHt9LFxuICAgXCJleGNlcHRpb25cIjogbnVsbCxcbiAgIFwiaW5wdXRfcGF0aFwiOiBcIlVudGl0bGVkLmlweW5iXCIsXG4gICBcIm91dHB1dF9wYXRoXCI6IFwiT3V0cHV0LmlweW5iXCIsXG4gICBcInBhcmFtZXRlcnNcIjoge30sXG4gICBcInN0YXJ0X3RpbWVcIjogXCIyMDE5LTEyLTE5VDA2OjI0OjU5Ljg2MzYzNFwiLFxuICAgXCJ2ZXJzaW9uXCI6IFwiMS4yLjFcIlxuICB9XG4gfSxcbiBcIm5iZm9ybWF0XCI6IDQsXG4gXCJuYmZvcm1hdF9taW5vclwiOiA0XG59IiwgImludGVydmFsIjogIm1pbnV0ZWx5IiwgImxldmVsIjogInByb2R1Y3Rpb24iLCAicmVwb3J0cyI6IDIsICJjcmVhdGVkIjogIjAxLzA5LzIwMjAgMTI6MTE6NTQiLCAibW9kaWZpZWQiOiAiMDEvMDkvMjAyMCAxMjoxMTo1NCJ9fQ=='))


dag = DAG('virtual-' + str(job_json['id']), default_args=default_operator_args, **dag_args)

def callable_virtualenv():
    """
    Example function that will be performed in a virtual environment.
    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    import nbschedule
    from nbschedule import worker, operator


    #import pip
    #installed_packages = pip.get_installed_distributions()
    #installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
    #for i in installed_packages
    #    print(installed_packages_list)


virtualenv_task = PythonVirtualenvOperator(
    task_id="virtualenv_python",
    python_callable=callable_virtualenv,
    requirements=[
        "colorama==0.4.0",
        "scir",
        "git+https://github.com/as-sher/papermill.git",
        "git+https://github.com/as-sher/nbschedule.git"
    ],
    system_site_packages=False,
    dag=dag,
)
