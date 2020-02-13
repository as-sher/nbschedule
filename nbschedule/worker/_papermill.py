import json
import os
import os.path
import nbformat
from six import string_types
from papermill import execute_notebook
from ._kernel import Kernel
from ._remotemagic import Remotemagic

try:
    from tempfile import TemporaryDirectory
except ImportError:
    from backports.tempfile import TemporaryDirectory


def run(nb_name, nb_text, parameters, hide_input, out_path, execution_details,report_id):
    '''Run the notebook and return the text

    Args:
        nb_name (string): Name of notebook
        nb_text (string): nbformat json of text of notebook to convert
        paramters (string): json parameters to use for papermill
        hide_input (boolean): hide code
    '''
    # validation for papermill
    # kernel_name = json.loads(nb_text)['metadata']['kernelspec']['name']
    kernel_name = (nb_text)['metadata']['kernelspec']['name']
    print("the kernel name is ")
    print(kernel_name)
    kernel = Kernel()
    if kernel_name == 'python3':
        kernel.install_ipykernel()
    elif kernel_name == 'pysparkkernel':
        kernel.install_pyspark_kernel()
    elif kernel_name == 'sparkkernel':
        kernel.install_spark_kernel()


    ##add support for executing remote notebooks.
    ##add check for execution details.

    remote_magic = Remotemagic()
    print(kernel_name)

    if (kernel_name == 'pysparkkernel' or kernel_name == 'sparkkernel') and execution_details is None:
        raise ValueError('execution details required')


    if (kernel_name == 'pysparkkernel' or kernel_name == 'sparkkernel'):
        nb = remote_magic.add_execution_details(nb_text,execution_details,'{}_{}'.format(nb_name,report_id))
    else:
        nb = nbformat.reads(json.dumps(nb_text),4)
    # nb = nbformat.reads(json.dumps(nb_text), 4)
    print("the value for nb is :-{}".format(nb))

    print("the cells are :- {}".format(nb['cells']))

    with TemporaryDirectory() as tempdir:
        in_file = os.path.join(tempdir, '{}.ipynb'.format(nb_name))
        out_file = os.path.join(out_path, '{}_report_{}_out.ipynb'.format(nb_name,report_id))

        nbformat.write(nb, in_file)



        if isinstance(parameters, string_types):
            parameters = json.loads(parameters)
        print("executing notebook")
        execute_notebook(in_file, out_file, parameters=parameters, report_mode=hide_input, start_timeout=600)

        with open(out_file, 'r') as fp:
            output_text = fp.read()

    return output_text
