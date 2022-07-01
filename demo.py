# --------------------------------------------------------------
# Imports
# --------------------------------------------------------------

# basic imports
from sys import argv
from prefect import Flow, Parameter, task
from prefect.tasks.secrets import EnvVarSecret

# specific task class imports
from sagetasks.synapse.prefect import (
    syn_bundle_client_args,
    syn_get_dataframe,
    syn_store_dataframe,
)
from sagetasks.sevenbridges.prefect import (
    sbg_bundle_client_args,
    sbg_get_project_id,
    sbg_get_imported_app_id,
    sbg_get_volume_id,
)


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
def print_values(series):
    print(series.tolist())


@task
def prepare_samplesheet(manifest):
    samplesheet = (
        manifest.pivot(
            index=["sample", "run", "strandedness"],
            columns="orientation",
            values="s3_uri",
        )
        .reset_index()
        .rename(columns={"R1": "fastq_1", "R2": "fastq_2"})
        .loc[:, ["sample", "fastq_1", "fastq_2", "strandedness"]]
    )
    return samplesheet


# --------------------------------------------------------------

# Open a Flow context and use the functional API (if possible)
# --------------------------------------------------------------

with Flow("Demo") as flow:

    # Parameters
    manifest_id = Parameter("manifest_id")
    samplesheet_parent = Parameter("samplesheet_parent")
    project_name = Parameter("project_name")
    billing_group_name = Parameter("billing_group_name")
    app_id = Parameter("app_id")
    volume_name = Parameter("volume_name")

    # Secrets
    syn_token = EnvVarSecret("SYNAPSE_AUTH_TOKEN")
    sbg_token = EnvVarSecret("SB_AUTH_TOKEN")

    # Client arguments
    syn_args = syn_bundle_client_args(syn_token)
    sbg_args = sbg_bundle_client_args(sbg_token)

    # Extract
    manifest = syn_get_dataframe(syn_args, manifest_id, sep=",")
    project_id = sbg_get_project_id(sbg_args, project_name, billing_group_name)
    app = sbg_get_imported_app_id(sbg_args, project_id, app_id)
    volume = sbg_get_volume_id(sbg_args, volume_name)

    # Transform
    samplesheet = prepare_samplesheet(manifest)
    head = head_df(manifest, 3)
    head_rows = split_rows(head)

    # Load
    print_columns(manifest)
    print_values.map(head_rows)
    syn_store_dataframe(syn_args, samplesheet, "samplesheet.csv", samplesheet_parent)

params = {
    "manifest_id": "syn31937724",
    "samplesheet_parent": "syn31937712",
    "project_name": "include-sandbox",
    "billing_group_name": "include-dev",
    "app_id": "cavatica/apps-publisher/kfdrc-rnaseq-workflow",
    "volume_name": "include_sandbox_ro",
}

if len(argv) < 2 or argv[1] == "run":
    flow.run(parameters=params)
elif argv[1] == "viz":
    flow.visualize(filename="flow", format="png")
elif argv[1] == "register":
    flow.register(project_name="demo")
else:
    print("Available Modes: 'run', 'viz', and 'register'")
