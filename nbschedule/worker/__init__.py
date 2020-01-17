import os
import os.path
import logging
from nbschedule.utils import name_to_class
from ._nbconvert import run as run_nbconvert  # noqa: F401
from ._papermill import run as run_papermill  # noqa: F401

def run(notebook_name, notebook_text, parameters_list, strip_code, output_path, working_dir=None):
    logging.critical('Calling run on job - %s' % job["id"])
    for i, parameters in enumerate(parameters_list, start=1):
        # type is "convert" for nbconversion jobs and "publish" for publish jobs
        # type = rep["meta"]["type"]

        # Papermill task, performs the report creation
        # using papermill and the report's individual
        # parameters and configuration
        papermilled = run_papermill(notebook_name,
                                    notebook_text,
                                    parameters,
                                    strip_code,
                                    output_path,
                                    i)
