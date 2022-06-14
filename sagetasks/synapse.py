"""Collection of Synapse-related Prefect tasks
"""

from tempfile import NamedTemporaryFile

from prefect import Task
import synapseclient
import pandas as pd


class SynapseBaseTask(Task):

    @property
    def synapse(self):
        return synapseclient.login(authToken=self.auth_token, silent=True)

    def run(self, auth_token):
        self.auth_token = auth_token
        raise NotImplementedError("The `run` method hasn't implemented.")


class SynapseGetDataFrameTask(SynapseBaseTask):

    def run(self, auth_token, synapse_id, sep=None):
        self.auth_token = auth_token
        file = self.synapse.get(synapse_id, downloadFile=False)
        file_handle_id = file._file_handle.id
        file_handle_url = self.synapse.restGET(
            f"/fileHandle/{file_handle_id}/url",
            self.synapse.fileHandleEndpoint,
            params={"redirect": False}
        )
        data_frame = pd.read_table(file_handle_url, sep=sep)
        return data_frame

class SynapseStoreDataFrameTask(SynapseBaseTask):

    def run(self, auth_token, data_frame, name, parent_id, sep=","):
        self.auth_token = auth_token
        with NamedTemporaryFile() as f:
            data_frame.to_csv(f, sep=sep)
            syn_file = synapseclient.File(f.name, name=name, parent=parent_id)
            syn_file = self.synapse.store(syn_file)
        return syn_file
