# --------------------------------------------------------------
# Imports
# --------------------------------------------------------------

# basic imports
from pathlib import PurePosixPath
from sys import argv
from prefect import Flow, Parameter, task, unmapped
from prefect.tasks.secrets import EnvVarSecret

# specific task function imports
import sagetasks.synapse.prefect as syn
import sagetasks.sevenbridges.prefect as sbg


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


@task(nout=2)
def prepare_file_imports(manifest):
    volume_paths = manifest.s3_uri.str.replace("s3://include-sandbox/synapse/", "", 1)
    project_paths = "synapse/" + manifest.component + "/" + manifest.filepath
    return (volume_paths.to_list(), project_paths.to_list())


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
    syn_args = syn.bundle_client_args(syn_token)
    sbg_args = sbg.bundle_client_args(sbg_token)

    # Extract
    manifest = syn.get_dataframe(syn_args, manifest_id, sep=",")
    project_id = sbg.get_project_id(sbg_args, project_name, billing_group_name)
    app_id = sbg.get_copied_app_id(sbg_args, project_id, app_id)
    volume_id = sbg.get_volume_id(sbg_args, volume_name)

    # Transform
    samplesheet = prepare_samplesheet(manifest)
    head = head_df(manifest, 3)
    # head_rows = split_rows(head)
    volume_paths, project_paths = prepare_file_imports(head)

    # Load
    # print_values(volume_paths)
    sbg.import_volume_file.map(
        unmapped(sbg_args),
        unmapped(project_id),
        unmapped(volume_id),
        volume_paths,
        project_paths,
    )
    syn.store_dataframe(syn_args, samplesheet, "samplesheet.csv", samplesheet_parent)

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
