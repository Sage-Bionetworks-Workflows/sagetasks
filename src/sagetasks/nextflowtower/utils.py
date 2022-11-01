from datetime import datetime

from sagetasks.nextflowtower.client import TowerClient
from sagetasks.utils import dedup, update_dict

ENDPOINTS = {
    "tower.nf": "https://tower.nf/api",
    "sage": "https://tower.sagebionetworks.org/api",
    "sage-dev": "https://tower-dev.sagebionetworks.org/api",
}


class TowerUtils:
    def __init__(self, client_args) -> None:
        """Initializes the Tower client with the bundled information.

        `client_args` can be generated with the `bundle_client_args()` static method.

        Optionally, you can set a default workspace for various methods with the
        `open_workspace()` method.
        """
        # TODO: Validate access token (by attempting a simple auth'ed request)
        self.client = TowerClient(**client_args)
        self._workspace = None

    @property
    def workspace(self):
        """Property for raising an error if a workspace hasn't been opened yet."""
        if self._workspace is None:
            raise ValueError("Workspace not opened yet. Use `open_workspace()`.")
        return self._workspace

    def open_workspace(self, workspace_id):
        """Sets the given workspace as the default for other methods."""
        self._workspace = workspace_id

    def close_workspace(self):
        """Clear the opened (default) workspace."""
        self._workspace = None

    def init_params(self):
        """Sets the given workspace as the default for other methods."""
        return {"workspaceId": self.workspace}

    @staticmethod
    def bundle_client_args(auth_token, platform="tower.nf", endpoint=None, **kwargs):
        """Bundles the information for authenticating a Tower client."""
        if platform is not None and platform not in ENDPOINTS:
            raise ValueError(f"`platform` must be among {list(ENDPOINTS)} (or None).")
        if platform is None and endpoint is None:
            raise ValueError(f"Provide value for either `platform` or `endpoint`.")
        endpoint = endpoint if endpoint else ENDPOINTS[platform]
        client_args = dict(tower_token=auth_token, tower_api_url=endpoint, **kwargs)
        return client_args

    def get_compute_env(self, compute_env_id):
        endpoint = f"/compute-envs/{compute_env_id}"
        params = self.init_params()
        response = self.client.request("GET", endpoint, params=params)
        compute_env = response["computeEnv"]
        return compute_env

    def get_workflow(self, workflow_id):
        endpoint = f"/workflow/{workflow_id}"
        params = self.init_params()
        response = self.client.request("GET", endpoint, params=params)
        return response

    def init_launch_workflow_data(self, compute_env_id):
        # Retrieve compute environment for default values
        compute_env = self.get_compute_env(compute_env_id)
        if compute_env["status"] != "AVAILABLE":
            ce_name = compute_env["name"]
            raise ValueError(f"The compute environment ({ce_name}) is not available.")
        ce_config = compute_env["config"]
        # Replicating date format in requests made by Tower frontend
        now_utc = datetime.now().isoformat()[:-3] + "Z"
        data = {
            "launch": {
                "computeEnvId": compute_env_id,
                "configProfiles": [],
                "configText": None,
                "dateCreated": now_utc,
                "entryName": None,
                "id": None,
                "mainScript": None,
                "paramsText": None,
                "pipeline": None,
                "postRunScript": ce_config["postRunScript"],
                "preRunScript": ce_config["preRunScript"],
                "pullLatest": None,
                "revision": None,
                "runName": None,
                "schemaName": None,
                "stubRun": None,
                "towerConfig": None,
                "userSecrets": [],
                "workDir": ce_config["workDir"],
                "workspaceSecrets": [],
            }
        }
        return data

    def launch_workflow(
        self,
        compute_env_id,
        pipeline,
        revision=None,
        params_yaml=None,
        params_json=None,
        nextflow_config=None,
        run_name=None,
        work_dir=None,
        profiles=(),
        workspace_secrets=(),
        user_secrets=(),
        pre_run_script=None,
        init_data=None,
    ):
        endpoint = "/workflow/launch"
        params = self.init_params()
        arguments = {
            "launch": {
                "configProfiles": profiles,
                "configText": nextflow_config,
                # TODO: Validate YAML or JSON
                "paramsText": params_yaml or params_json,
                "pipeline": pipeline,
                "preRunScript": pre_run_script,
                "revision": revision,
                # TODO: Avoid duplicate run names
                "runName": run_name,
                "userSecrets": dedup(user_secrets),
                "workDir": work_dir,
                "workspaceSecrets": dedup(workspace_secrets),
            }
        }
        # Update default data with argument values and user-provided overrides
        data = init_data or self.init_launch_workflow_data(compute_env_id)
        data = update_dict(data, arguments)
        # Launch workflow and obtain workflow ID
        response = self.client.request("POST", endpoint, params=params, json=data)
        # Get more information about workflow run
        workflow = self.get_workflow(response["workflowId"])
        return workflow
