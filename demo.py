# --------------------------------------------------------------
# Imports
# --------------------------------------------------------------

# basic imports
from sys import argv
from prefect import Flow, Parameter, task
from prefect.tasks.secrets import EnvVarSecret

# specific task class imports
from sagetasks.synapse import (
    SynapseClientTask,
    SynapseGetDataFrameTask,
    SynapseStoreDataFrameTask,
)
from sagetasks.cavatica import (
    CavaticaClientTask,
    CavaticaGetProjectTask,
    CavaticaGetAppTask,
)

# Constants
SB_API_ENDPOINT = "https://cavatica-api.sbgenomics.com/v2"


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
# Instantiate task classes
# --------------------------------------------------------------

# Synapse tasks
synapse_login = SynapseClientTask(name="Create authenticated Synapse client")
get_manifest = SynapseGetDataFrameTask(name="Download manifest file from Synapse")
upload_samplesheet = SynapseStoreDataFrameTask(name="Upload sample sheet to Synapse")

# Cavatica tasks
cavatica_login = CavaticaClientTask(name="Create authenticated Cavatica client")
get_project = CavaticaGetProjectTask(name="Retrieve Cavatica project")
get_app = CavaticaGetAppTask(name="Retrieve Cavatica app")

# --------------------------------------------------------------
# Open a Flow context and use the functional API (if possible)
# --------------------------------------------------------------

with Flow("Demo") as flow:

    # Parameters
    manifest_id = Parameter("manifest_id")
    samplesheet_parent = Parameter("samplesheet_parent")
    project_name = Parameter("project_name")
    billing_group_id = Parameter("billing_group_id")
    app_id = Parameter("app_id")

    # Secrets
    syn_token = EnvVarSecret("SYNAPSE_AUTH_TOKEN")
    sbg_token = EnvVarSecret("SB_AUTH_TOKEN")

    # Clients
    syn = synapse_login(syn_token)
    sbg = cavatica_login(SB_API_ENDPOINT, sbg_token)

    # Extract
    manifest = get_manifest(syn, manifest_id, sep=",")
    project = get_project(sbg, project_name, billing_group_id)
    app = get_app(sbg, app_id, project)

    # Transform
    samplesheet = prepare_samplesheet(manifest)
    head = head_df(manifest, 3)
    head_rows = split_rows(head)

    # Load
    print_columns(manifest)
    print_values.map(head_rows)
    upload_samplesheet(syn, samplesheet, "samplesheet.csv", samplesheet_parent)

params = {
    "manifest_id": "syn31937724",
    "samplesheet_parent": "syn31937712",
    "project_name": "sandbox",
    "billing_group_id": "6428bd01-8c8a-4d57-b18d-be5632f701ed",
    "app_id": "cavatica/apps-publisher/kfdrc-rnaseq-workflow",
}

if len(argv) < 2 or argv[1] == "run":
    flow.run(parameters=params)
elif argv[1] == "viz":
    flow.visualize(filename="flow", format="png")
elif argv[1] == "register":
    flow.register(project_name="demo")
else:
    print("Available Modes: 'run', 'viz', and 'register'")
