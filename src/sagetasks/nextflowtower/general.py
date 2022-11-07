from sagetasks.nextflowtower.utils import TowerUtils


def bundle_client_args(auth_token, platform="sage", endpoint=None, **kwargs):
    """Nextflow Tower - Bundle client arguments."""
    return TowerUtils.bundle_client_args(auth_token, platform, endpoint, **kwargs)


def launch_workflow(
    compute_env_id: str,
    pipeline: str,
    client_args=None,
    workspace_id=None,
):
    """Nextflow Tower - Launch a workflow."""
    client_args = client_args or dict()
    utils = TowerUtils(client_args)
    utils.open_workspace(workspace_id)
    workflow = utils.launch_workflow(
        compute_env_id,
        pipeline,
        # revision=None,
        # params_yaml=None,
        # params_json=None,
        # nextflow_config=None,
        # run_name=None,
        # work_dir=None,
        # profiles=(),
        # user_secrets=(),
        # workspace_secrets=(),
        # pre_run_script=None,
        # init_data=None,
    )
    return workflow
