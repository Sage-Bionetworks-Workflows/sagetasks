"""Collection of Synapse-related Prefect tasks
"""

import os
from tempfile import TemporaryDirectory

from prefect import Task
import synapseclient
import pandas as pd


CONTENT_TYPES = {",": "text/csv", "\t": "text/tab-separated-values"}


class SynapseBaseTask(Task):
    def get_client(self, client_args):
        return synapseclient.login(**client_args)

    def run(self):
        raise NotImplementedError("The `run` method isn't implemented.")


class SynapseClientArgsTask(SynapseBaseTask):
    def run(self, auth_token, **kwargs):
        client_args = dict(authToken=auth_token, silent=True)
        client_args.update(kwargs)
        return client_args


class SynapseGetDataFrameTask(SynapseBaseTask):
    def run(self, client_args, synapse_id, sep=None):
        client = self.get_client(client_args)
        file = client.get(synapse_id, downloadFile=False)
        file_handle_id = file._file_handle.id
        file_temp_url = client.restGET(
            f"/file/{file_handle_id}",
            client.fileHandleEndpoint,
            params={
                "fileAssociateId": synapse_id,
                "fileAssociateType": "FileEntity",
                "redirect": False,
            },
        )
        data_frame = pd.read_table(file_temp_url, sep=sep)
        return data_frame


class SynapseStoreDataFrameTask(SynapseBaseTask):
    def run(self, client_args, data_frame, name, parent_id, sep=","):
        client = self.get_client(client_args)
        with TemporaryDirectory() as dirname:
            fpath = os.path.join(dirname, name)
            content_type = CONTENT_TYPES.get(sep, None)
            with open(fpath, "w") as f:
                data_frame.to_csv(f, sep=sep, index=False)
            syn_file = synapseclient.File(
                fpath, name=name, parent=parent_id, contentType=content_type
            )
            syn_file = client.store(syn_file)
            os.remove(fpath)
        return syn_file
