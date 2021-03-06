import nbformat

import json
import base64
import json

def get_execution_conf(execution_details,session_name,kernel_name):
    encoded_conf = base64.b64encode(json.dumps(execution_details).encode())
    if kernel_name == "pysparkkernel":
        language = "python"
    else:
        language = "scala"
    print("the language is {}".format(language))
    ext_conf = "%spark encoded -s {} -l {} -f {}".format(session_name,language,encoded_conf.decode("utf-8"))
    conf = "{\"execution_count\": null, \"cell_type\": \"code\", \"source\": [\"%s\",\"%s\"], \"outputs\": [], \"metadata\": {}}"%(r"%load_ext sparkmagic.magics\n",ext_conf)
    execution_conf = json.loads(conf)
    return execution_conf


class Remotemagic():

    def add_execution_details(self, nb_text, execution_details, session_name):
        nb = nbformat.reads(json.dumps(nb_text),4)
        kernel_name = (nb_text)['metadata']['kernelspec']['name']
        execution_conf = get_execution_conf(execution_details, session_name, kernel_name)
        final = nbformat.from_dict(execution_conf)
        nb['cells'].insert(0,final)
        return nb