import json
import os
import os.path
import nbformat
from six import string_types
from papermill import execute_notebook
from ._kernel import Kernel

try:
    from tempfile import TemporaryDirectory
except ImportError:
    from backports.tempfile import TemporaryDirectory


def run(nb_name, nb_text, parameters, hide_input, out_path, report_id):
    '''Run the notebook and return the text

    Args:
        nb_name (string): Name of notebook
        nb_text (string): nbformat json of text of notebook to convert
        paramters (string): json parameters to use for papermill
        hide_input (boolean): hide code
    '''
    # validation for papermill
    kernel_name = json.loads(nb_text)['metadata']['kernelspec']['name']
    print(kernel_name)
    kernel = Kernel()
    if kernel_name == 'python3':
        kernel.install_ipykernel()
    elif kernel_name == 'pysparkkernel':
        kernel.install_pyspark_kernel()
    elif kernel_name == 'sparkkernel':
        kernel.install_spark_kernel()

    nb = nbformat.reads(nb_text, 4)

    with TemporaryDirectory() as tempdir:
        in_file = os.path.join(tempdir, '{}.ipynb'.format(nb_name))
        out_file = os.path.join(out_path, '{}_report_{}_out.ipynb'.format(nb_name,report_id))

        nbformat.write(nb, in_file)

        if isinstance(parameters, string_types):
            parameters = json.loads(parameters)

        execute_notebook(in_file, out_file, parameters=parameters, report_mode=hide_input, start_timeout=600)

        with open(out_file, 'r') as fp:
            output_text = fp.read()

    return output_text
