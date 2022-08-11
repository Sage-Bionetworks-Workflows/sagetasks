# Sage Prefect Tasks

⚠️ **Warning: This repository is a work in progress.** ⚠️

Python package of useful Prefect tasks for common use cases at Sage Bionetworks.

Some thoughts are included below the Demo Flow and Usage.

Inspired by [Pocket/data-flows](https://github.com/Pocket/data-flows).

## Demo Flow

![Demo Flow](flow.png)

## Demo Usage

### Getting access

To run this demo, you'll need the following access:

- You need to ask Bruno for edit-access on the [INCLUDE Sandbox](https://www.synapse.org/#!Synapse:syn31937702/wiki/) Synapse project.
- You need to ask Bruno for edit-access on the [include-sandbox](https://cavatica.sbgenomics.com/u/bgrande/include-sandbox) Cavatica project.

### Getting set up

```sh
# Create a virtual environment with the Python dependencies
pipenv install

# Copy the example `.env` file and update the auth tokens
cp .env.example .env
```

### Run the flow at the command line

You'll need to [get set up](#getting-set-up) first.

```sh
# Run the demo (pipenv will automatically load the `.env` file)
pipenv run python demo.py
```

### Inspect the flow using the Prefect Server UI

You'll need to [get set up](#getting-set-up) first.

```sh
# Deploy Prefect Server in the background
prefect server start --detach

# Create a project in Prefect Server
prefect create project "demo"

# Run the demo in "register" mode
pipenv run python demo.py register

# Explore the flow under the demo project in Prefect Server
# Usually hosted at http://127.0.0.1:8080/

# Stop the running containers
prefect server stop
```

## Thoughts

- The `CavaticaBaseTask` demonstrates a use case for classes (_i.e._ extending `Task`) as opposed to functions (_i.e._ decorated by `@task`). On the other hand, `SynapseBaseTask` doesn't really benefit from the class structure.

- The SevenBridges Python client embeds the client instance into every resource object, which prevents `cloudpickle` to serialize these objects due to `TypeError: cannot pickle '_thread.lock' object`.

  ```python
  import os
  import cloudpickle
  import sevenbridges as sbg
  api = sbg.Api(url="https://cavatica-api.sbgenomics.com/v2", token=os.environ["SB_AUTH_TOKEN"])
  proj = api.projects.query(name="include-sandbox")[0]
  proj._API = None
  proj._api = None
  proj._data.api = None
  pickle = cloudpickle.dumps(proj)
  ```


## Operating in AWS


### Setting up EC2 Agent + Prefect work queue

1. Create prefect cloud 2.0 account and create a workspace
1. Create work queue on prefect cloud in the UI.  **Note**: there is a command to create work queues via prefect cli.
1. Create service catalog EC2
1. Create prefect cloud API Key, and in the ec2, log into prefect cloud and select workspace.

    ```
    # Running this command will set this value in ~/.prefect/profiles.toml
    prefect cloud login -k <prefect API key>
    ```

1. Install all dependencies your flows require + more.  Should set up `Docker` to support Docker deployments (instructions not included here).

    ```
    pip install prefect-aws
    pip install s3fs
    pip install -U prefect
    ```

1. Get work queue id from UI and start agent on EC2. **Note**: there is a command to get the work queue ids via prefect cli.
    ```
    prefect agent start <work queue id>
    ```

### Creating deployment that links to S3 object

1. Create s3 bucket in an AWS account.  I chose org-sagebase-dnt-dev and created bucket: `tyu-prefect-test`.
1. Create IAM user and added read and write only permissions on the bucket.  Policy [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)
1. Set up AWS profile and check that credentials work. Access key id and secret access key saved in lastpass under `tyu-s3-prefect-aws-key`.

    ```
    aws --profile <your-profile> s3 ls s3://tyu-prefect-test
    ```

1. Create Prefect Block on prefect UI. The block name is called `tyu-test-prefect`.  **Note**: This is probably possible via prefect cli.
1. Create s3 deployment https://discourse.prefect.io/t/how-to-deploy-prefect-2-0-flows-to-aws/1252

    ```
    prefect deployment build flow/basic_flow.py:basic_flow --name test-s3-basic-flow --tag dev --storage-block s3/tyu-test-prefect/basic_flow
    prefect deployment apply flow/basic_flow-deployment.yaml
    ```

1. Execute deployment in prefect cloud UI. This should read from the s3 object with the deployments and deploy to the agent set up on the ec2.  **Note**: there is an ability to trigger deployments via prefect cli.

### Concerns

1. When creating a deployment using a prefect S3 block, the entire directory where the flow exists gets uploaded and you can’t specify a folder to upload the flows into.  There are some solutions to this
    * Upload python script and relevant files using `aws cli` into its own respective folders in S3 and create the prefect deployment.yaml “manually”
    * Installing dependencies on agent or in a docker container so that only one flow file is needed in the S3 bucket
    * Different S3 buckets for different prefect deployments
1. Prefect by design is “one pod or container” per Flow.  If was true in airflow, this would mean that there aren’t Docker/Kubernetes Operators but everything is on the DAG level.  From a prefect chat, a prefect engineer states “why would you need each task to run in a separate image? this makes things so much more difficult - e.g. you can’t pass data between tasks in memory this way since each of them runs in a different process”.
    * I think they are working on “sub flows” which should enable a “Docker Task”, but essentially the way to achieve something similar to a Kubernetes/Docker operator using prefect is something like this.
    * You need to install prefect within the docker container for the Docker/kubernetes offering it to work (see docs).
1. Missing `s3fs` dependency when uploading to s3
1. Potential risk when creating S3 blocks, beacause the aws credentials are stored within the Block.  Not sure if this information is stored on agent or within prefect cloud database.
