import os
import os.path
import logging
from nbschedule.utils import name_to_class
from ._nbconvert import run as run_nbconvert  # noqa: F401
from ._papermill import run as run_papermill  # noqa: F401

def run(job, reports, working_dir=None):
    logging.critical('Calling run on job - %s' % str(job.id))
    for rep in reports:
        # type is "convert" for nbconversion jobs and "publish" for publish jobs
        type = rep.meta.type

        # Papermill task, performs the report creation
        # using papermill and the report's individual
        # parameters and configuration
        papermilled = run_papermill(rep.meta.notebook.name,
                                    rep.meta.notebook.meta.notebook,
                                    rep.meta.parameters,
                                    rep.meta.strip_code,
                                    rep.meta.output_path,
                                    rep.id)
