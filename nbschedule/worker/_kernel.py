from pip._vendor import pkg_resources
from jupyter_client.kernelspecapp import KernelSpecManager
from pip._vendor import pkg_resources
from ipykernel.kernelspec import install

class Kernel():

    def __init__(self):
        location = None
        for pkg in pkg_resources.working_set:
            if pkg.project_name == "sparkmagic":
                location = pkg.location
                break
        self.location = location

    def install_spark_kernel(self):
        KernelSpecManager().install_kernel_spec(source_dir= self.location + "/sparkmagic/kernels/sparkkernel", user=True)
        print('done')

    def install_pyspark_kernel(self):
        KernelSpecManager().install_kernel_spec(source_dir= self.location + "/sparkmagic/kernels/pysparkkernel", user=True)
        print('done')

    def install_ipykernel(self):
        install(user=True)
        print('done')