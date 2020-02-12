import nbformat

import json
import base64
import json

def get_execution_conf(execution_details,session_name):
    encoded_conf = base64.b64encode(json.dumps(execution_details).encode())
    ext_conf = "%spark encoded -s {} -l {} -f {}".format(session_name,"python",encoded_conf)
    conf = "{\"execution_count\": null, \"cell_type\": \"code\", \"source\": [\"%s\"], \"outputs\": [], \"metadata\": {}}"%(ext_conf)
    execution_conf = json.loads(conf)
    return execution_conf


class Remotemagic():

    def add_execution_details(self, nb_text, execution_details, session_name):
        nb = nbformat.reads(json.dumps(nb_text),4)
        execution_conf = get_execution_conf(execution_details, session_name)
        final = nbformat.from_dict(execution_conf)
        nb['cells'].append(final)
        return nb