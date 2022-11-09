from typing import List, Optional

from sagetasks.nextflowtower.utils import TowerUtils

# TODO: Re-enable this function once we've figured out how to best handle
#       `kwargs` in Typer. The following links might be useful for this:
#       https://typer.tiangolo.com/tutorial/commands/context/#configuring-the-context
#       https://peps.python.org/pep-0692/
# def bundle_client_args(auth_token, platform="sage", endpoint=None, **kwargs):
#     """Nextflow Tower - Bundle client arguments."""
#     return TowerUtils.bundle_client_args(auth_token, platform, endpoint, **kwargs)


def launch_workflow(
    compute_env_id: str,
    pipeline: str,
    workspace_id=None,
    revision: Optional[str] = None,
    params_yaml: Optional[str] = None,
    params_json: Optional[str] = None,
    nextflow_config: Optional[str] = None,
    run_name: Optional[str] = None,
    work_dir: Optional[str] = None,
    profiles: Optional[List[str]] = None,
    user_secrets: Optional[List[str]] = None,
    workspace_secrets: Optional[List[str]] = None,
    pre_run_script: Optional[str] = None,
    # TODO: Re-enable once we find a way to get Typer to support Mappings
    # init_data: Optional[Mapping] = None,
    client_args=None,
):
    """Launch a workflow run on Nextflow Tower.

    You can provide your Tower credentials with the following
    environment variables:

    - NXF_TOWER_TOKEN='<tower-access-token>'

    - NXF_TOWER_API_URL='<tower-api-url>'



    You can optionally enable debug mode (HTTP request logs)
    with the following environment variable:

    - NXF_TOWER_DEBUG=1
    """
    # More specific default values than None
    client_args = client_args or dict()
    profiles = profiles or ()
    user_secrets = user_secrets or ()
    workspace_secrets = workspace_secrets or ()
    # Prepare and execute the workflow launch
    utils = TowerUtils(client_args)
    utils.open_workspace(workspace_id)
    workflow = utils.launch_workflow(
        compute_env_id,
        pipeline,
        revision=revision,
        params_yaml=params_yaml,
        params_json=params_json,
        nextflow_config=nextflow_config,
        run_name=run_name,
        work_dir=work_dir,
        profiles=profiles,
        user_secrets=user_secrets,
        workspace_secrets=workspace_secrets,
        pre_run_script=pre_run_script,
    )
    return workflow
