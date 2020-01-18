import json
import os
import os.path
import logging
from nbschedule.utils import name_to_class
from ._nbconvert import run as run_nbconvert  # noqa: F401
from ._papermill import run as run_papermill  # noqa: F401
from base64 import b64decode

def run(job_json, working_dir=None):

    notebook_json = json.loads(b64decode(job_json).decode('utf-8'))

    notebook_name = notebook_json['name']
    notebook_text = notebook_json['notebook_text']
    parameters_list = notebook_json['parameters_list']
    output_path = notebook_json['output_path']
    logging.critical('Calling run on job - %s' % notebook_name)
    for i, parameters in enumerate(parameters_list, start=1):
        # type is "convert" for nbconversion jobs and "publish" for publish jobs
        # type = rep["meta"]["type"]
        # Papermill task, performs the report creation
        # using papermill and the report's individual
        # parameters and configuration
        papermilled = run_papermill(notebook_name,
                                    notebook_text,
                                    parameters,
                                    False,
                                    output_path,
                                    i)
