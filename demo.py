#--------------------------------------------------------------
# Imports
#--------------------------------------------------------------

# basic imports
from prefect import Flow, Parameter, task
from prefect.tasks.secrets import EnvVarSecret

# specific task class imports
from sagetasks.synapse import SynapseGetDataFrameTask, SynapseStoreDataFrameTask


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

get_manifest = SynapseGetDataFrameTask(name='Download manifest file from Synapse')

upload_samplesheet = SynapseStoreDataFrameTask(name='Upload sample sheet to Synapse')

#--------------------------------------------------------------
# Open a Flow context and use the functional API (if possible)
#--------------------------------------------------------------

with Flow('Demo') as flow:
    manifest_id = Parameter('manifest_id')
    auth_token = EnvVarSecret("SYNAPSE_AUTH_TOKEN")
    manifest = get_manifest(auth_token, manifest_id, ",")
    print_columns(manifest)
    upload_samplesheet(auth_token, manifest, "samplesheet.csv", "syn31937712")

params = {"manifest_id": "syn31937724"}
flow.run(parameters=params)

# Or register the flow after launching Prefect Server with:
#    prefect server start
#    prefect create project "demo"
# flow.register(project_name="demo")
