# --------------------------------------------------------------
# Imports
# --------------------------------------------------------------

# basic imports
import os
from sys import argv

import pandas as pd
from prefect import flow, task, unmapped
from prefect.blocks.system import Secret

import sagetasks.sevenbridges.general as sbggen
import sagetasks.sevenbridges.prefect as sbg

# specific task function imports
import sagetasks.synapse.prefect as syn
from sagetasks.sevenbridges.inputs import manifest_to_kf_rnaseq_app_inputs_factory

# --------------------------------------------------------------
# Generate custom functions using factories
# --------------------------------------------------------------


prepare_task_inputs = manifest_to_kf_rnaseq_app_inputs_factory()


# --------------------------------------------------------------
# Define custom task functions
# --------------------------------------------------------------


@task
def print_columns(data_frame):
    print(data_frame.columns.tolist())


@task
def head_df(data_frame, n):
    return data_frame.head(n)


@task
def split_rows(data_frame):
    return [row for _, row in data_frame.iterrows()]


@task
def print_values(x):
    print(f"\n{x}\n")


@task
def prepare_file_imports(row):
    s3_uri_prefix = "s3://include-sandbox/synapse/"
    row["volume_path"] = row.s3_uri.replace(s3_uri_prefix, "", 1)
    row["project_path"] = "synapse/" + row.component + "/" + row.filepath
    return row


@task
def call_import_volume_file(sbg_args, project_id, volume_id, row):
    imported_file_id = sbggen.import_volume_file(
        sbg_args,
        project_id,
        volume_id,
        row.volume_path,
        row.project_path,
    )
    row["cavatica_file_id"] = imported_file_id
    return row


@task
def concat_rows(rows):
    return pd.concat(rows, axis=1).T


# --------------------------------------------------------------
# Open a Flow context and use the functional API (if possible)
# --------------------------------------------------------------


@flow()
def populate_secrets():
    synapse_secret = Secret(value=os.environ["SYNAPSE_AUTH_TOKEN"])
    synapse_secret.save(name="synapse-auth-token", overwrite=True)
    cavatica_secret = Secret(value=os.environ["SB_AUTH_TOKEN"])
    cavatica_secret.save(name="sb-auth-token", overwrite=True)


@flow(name="Demo")
def demo(
    manifest_id, synapse_folder, project_name, billing_group_name, app_id, volume_name
):

    # Secrets
    syn_token = Secret.load("synapse-auth-token").get()
    sbg_token = Secret.load("sb-auth-token").get()

    # Client arguments
    syn_args = syn.bundle_client_args(syn_token)
    sbg_args = sbg.bundle_client_args(sbg_token)

    # Extract
    manifest = syn.get_dataframe(syn_args, manifest_id, sep=",")
    project_id = sbg.get_project_id(sbg_args, project_name, billing_group_name)
    app_id = sbg.get_copied_app_id(sbg_args, project_id, app_id)
    volume_id = sbg.get_volume_id(sbg_args, volume_name)

    # Transform
    rows = split_rows(manifest)
    rows_prep = prepare_file_imports.map(rows)

    # Load
    rows_imp = call_import_volume_file.map(
        unmapped(sbg_args),
        unmapped(project_id),
        unmapped(volume_id),
        rows_prep,
    )
    sbg_manifest = concat_rows(rows_imp)
    drafted_tasks = sbg.create_tasks(
        sbg_args, project_id, app_id, sbg_manifest, prepare_task_inputs
    )
    print_values(drafted_tasks)
    syn.store_dataframe(syn_args, sbg_manifest, "sbg_manifest.csv", synapse_folder)


params = {
    "manifest_id": "syn33335513",
    "synapse_folder": "syn33335225",
    "project_name": "include-sandbox",
    "billing_group_name": "include-dev",
    "app_id": "cavatica/apps-publisher/kfdrc-rnaseq-workflow",
    "volume_name": "include_sandbox_ro",
}

if __name__ == "__main__":
    if len(argv) < 2 or argv[1] == "run":
        populate_secrets()
        demo(**params)
    elif argv[1] == "viz":
        raise NotImplementedError("Flow visualization not available yet in Prefect 2")
    else:
        print("Available Modes: 'run' and 'viz'")
