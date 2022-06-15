#--------------------------------------------------------------
# Imports
#--------------------------------------------------------------

# basic imports
from prefect import Flow, Parameter, task
from prefect.tasks.secrets import EnvVarSecret

# specific task class imports
from sagetasks.synapse import SynapseGetDataFrameTask, SynapseStoreDataFrameTask
from sagetasks.cavatica import CavaticaGetProjectTask, CavaticaGetAppTask

# Constants
SB_API_ENDPOINT = "https://cavatica-api.sbgenomics.com/v2"


#--------------------------------------------------------------
# Define custom task functions
#--------------------------------------------------------------

@task
def print_columns(data_frame):
    print(data_frame.columns.tolist())

@task
def prepare_samplesheet(manifest):
    samplesheet = (
        manifest
        .pivot(index=['sample', 'run', 'strandedness'],
               columns='orientation', values='s3_uri')
        .reset_index()
        .rename(columns={"R1": "fastq_1", "R2": "fastq_2"})
        .loc[:,["sample", "fastq_1", "fastq_2", "strandedness"]]
    )
    return samplesheet

#--------------------------------------------------------------
# Instantiate task classes
#--------------------------------------------------------------

# Synapse tasks
get_manifest = SynapseGetDataFrameTask(name='Download manifest file from Synapse')
upload_samplesheet = SynapseStoreDataFrameTask(name='Upload sample sheet to Synapse')

# Cavatica tasks
get_project = CavaticaGetProjectTask(SB_API_ENDPOINT, name='Retrieve Cavatica project')
get_app = CavaticaGetAppTask(SB_API_ENDPOINT, name='Retrieve Cavatica app')

#--------------------------------------------------------------
# Open a Flow context and use the functional API (if possible)
#--------------------------------------------------------------

with Flow('Demo') as flow:

    # Parameters
    manifest_id = Parameter('manifest_id')
    samplesheet_parent = Parameter('samplesheet_parent')
    project_name = Parameter('project_name')
    billing_group_id = Parameter('billing_group_id')
    app_id = Parameter('app_id')

    # Secrets
    syn_token = EnvVarSecret("SYNAPSE_AUTH_TOKEN")
    sbg_token = EnvVarSecret("SB_AUTH_TOKEN")

    # Extract
    manifest = get_manifest(syn_token, manifest_id, sep=",")
    project = get_project(sbg_token, project_name, billing_group_id)
    app = get_app(sbg_token, app_id, project)

    # Transform
    samplesheet = prepare_samplesheet(manifest)

    # Load
    print_columns(manifest)
    upload_samplesheet(syn_token, samplesheet, "samplesheet.csv", samplesheet_parent)

params = {
    "manifest_id": "syn31937724",
    "samplesheet_parent": "syn31937712",
    "project_name": "sandbox",
    "billing_group_id": "6428bd01-8c8a-4d57-b18d-be5632f701ed",
    "app_id": "cavatica/apps-publisher/kfdrc-rnaseq-workflow"
}
flow.visualize(filename="flow", format="png")
flow.run(parameters=params)

# Or register the flow after launching Prefect Server with:
#    prefect server start
#    prefect create project "demo"
# flow.register(project_name="demo")
