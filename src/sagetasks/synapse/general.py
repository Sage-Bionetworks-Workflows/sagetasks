import os
from tempfile import TemporaryDirectory

import pandas as pd
import synapseclient

CONTENT_TYPES = {",": "text/csv", "\t": "text/tab-separated-values"}


def bundle_client_args(auth_token, **kwargs):
    """Synapse - Bundle client arguments"""
    client_args = dict(authToken=auth_token, silent=True)
    client_args.update(kwargs)
    return client_args


def get_dataframe(client_args, synapse_id, sep=None):
    """Synapse - Download and load data frame"""
    client = synapseclient.login(**client_args)
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


def store_dataframe(client_args, data_frame, name, parent_id, sep=","):
    """Synapse - Serialize and upload data frame"""
    client = synapseclient.login(**client_args)
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
