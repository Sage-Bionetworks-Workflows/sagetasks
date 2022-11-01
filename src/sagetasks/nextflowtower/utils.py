from datetime import datetime
from typing import Mapping, Optional, Sequence

from sagetasks.nextflowtower.client import TowerClient
from sagetasks.utils import dedup, update_dict

ENDPOINTS = {
    "tower.nf": "https://tower.nf/api",
    "sage": "https://tower.sagebionetworks.org/api",
    "sage-dev": "https://tower-dev.sagebionetworks.org/api",
}


class TowerUtils:
    def __init__(
        self, client_args: Mapping, workspace_id: Optional[int] = None
    ) -> None:
        """Initialize TowerUtils for interacting with the Tower API.

        Args:
            client_args (Mapping): Bundled client arguments, which can
                be generated with the `TowerUtils.bundle_client_args()`
                static method.
            workspace (int, optional): Tower workspace identifier.
                Defaults to None with no opened workspace.
        """
        # TODO: Validate access token (by attempting a simple auth'ed request)
        self.client = TowerClient(**client_args)
        self._workspace: Optional[int] = None
        self.open_workspace(workspace_id)

    @property
    def workspace(self) -> int:
        """Retrieve default workspace for workspace-related requests.

        Raises:
            ValueError: If a workspace isn't open. You can open a workspace
                when initializing `TowerUtils()` or with `open_workspace()`.

        Returns:
            int: Tower workspace identifier.
        """
        if self._workspace is None:
            raise ValueError("Workspace not opened yet. Use `open_workspace()`.")
        return self._workspace

    def open_workspace(self, workspace_id: Optional[int]) -> None:
        """Configure default workspace for workspace-related requests.

        Args:
            workspace_id (Optional[int]): Tower workspace identifier.
                If `None`, this will close any opened workspace.
        """
        if workspace_id is None:
            self.close_workspace()
        # TODO: Validate workspace
        else:
            self._workspace = int(workspace_id)

    def close_workspace(self) -> None:
        """Clear default workspace for workspace-related requests."""
        self._workspace = None

    def init_params(self) -> dict:
        """Initialize query-string parameters with workspace ID.

        Returns:
            dict: Parameters for workspace-related requests,
                which can be passed to `requests.request`.
        """
        return {"workspaceId": self.workspace}

    @staticmethod
    def bundle_client_args(
        auth_token: Optional[str] = None,
        platform: Optional[str] = "sage",
        endpoint: Optional[str] = None,
        **kwargs,
    ) -> dict:
        """Bundle the information needed for authenticating a Tower client.

        Args:
            auth_token (str, optional): Tower access token, which gets
                included in the HTTP header as an Authorization Bearer
                token. Defaults to None, which prompts the use of the
                `NXF_TOWER_TOKEN` environment variable.
            platform (str, optional): Compact identifier for commonly
                used platforms to populate the endpoint. Options include
                "sage" and "tower.nf". Defaults to "sage".
            endpoint (str, optional): Full Tower API URL. This argument
                will override the value associated with the specified
                `platform`. Defaults to None.

        Raises:
            ValueError: If `platform` is not valid.
            ValueError: If `platform` and `endpoint` are not provided.

        Returns:
            dict: Bundle of Tower client arguments.
        """
        if platform is not None and platform not in ENDPOINTS:
            raise ValueError(f"`platform` must be among {list(ENDPOINTS)} (or None).")
        if platform and not endpoint and platform in ENDPOINTS:
            endpoint = ENDPOINTS[platform]
        if endpoint is None:
            raise ValueError("You must provide a value to `platform` or `endpoint`.")
        client_args = dict(tower_token=auth_token, tower_api_url=endpoint, **kwargs)
        return client_args

    def get_compute_env(self, compute_env_id: str) -> dict:
        """Retrieve information about a given compute environment.

        Args:
            compute_env_id (str): Compute environment alphanumerical ID.

        Returns:
            dict: Information about the compute environment.
        """
        endpoint = f"/compute-envs/{compute_env_id}"
        params = self.init_params()
        response = self.client.request("GET", endpoint, params=params)
        compute_env = response["computeEnv"]
        return compute_env

    def get_workflow(self, workflow_id: str) -> dict:
        """Retrieve information about a given workflow run.

        Args:
            workflow_id (str): Workflow run alphanumerical ID.

        Returns:
            dict: Information about the workflow run.
        """
        endpoint = f"/workflow/{workflow_id}"
        params = self.init_params()
        response = self.client.request("GET", endpoint, params=params)
        return response

    def init_launch_workflow_data(self, compute_env_id: str) -> dict:
        """Initialize request for `/workflow/launch` endpoint.

        You can use this method to modify the contents before
        passing the adjusted payload to the `init_data` argument
        on the `launch_workflow()` method.

        Args:
            compute_env_id (str): Compute environment alphanumerical ID.

        Raises:
            ValueError: If the compute environment is not available.

        Returns:
            dict: Initial request for `/workflow/launch` endpoint.
        """
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
        compute_env_id: str,
        pipeline: str,
        revision: Optional[str] = None,
        params_yaml: Optional[str] = None,
        params_json: Optional[str] = None,
        nextflow_config: Optional[str] = None,
        run_name: Optional[str] = None,
        work_dir: Optional[str] = None,
        profiles: Optional[Sequence] = (),
        user_secrets: Optional[Sequence] = (),
        workspace_secrets: Optional[Sequence] = (),
        pre_run_script: Optional[str] = None,
        init_data: Optional[Mapping] = None,
    ) -> dict:
        """Launch a workflow using the given compute environment.

        This method will use any opened workspace if available.

        Args:
            compute_env_id (str): Compute environment ID where the
                execution will be launched.
            pipeline (str): Nextflow pipeline URL. This can be a GitHub
                shorthand like `nf-core/rnaseq`.
            revision (str, optional): A valid repository commit ID (SHA),
                tag, or branch name. Defaults to None.
            params_yaml (str, optional): Pipeline parameters in YAML format.
                Defaults to None.
            params_json (str, optional): Pipeline parameters in JSON format.
                Defaults to None.
            nextflow_config (str, optional): Additional Nextflow configuration
                settings can be provided here. Defaults to None.
            run_name (str, optional): Custom workflow run name. Defaults to
                None, which will automatically assign a random run name.
            work_dir (str, optional): The bucket path where the pipeline
                scratch data is stored. Defaults to None, which uses the
                default work directory for the given compute environment.
            profiles (Sequence, optional): Configuration profile names
                to use for this execution. Defaults to an empty list.
            user_secrets (Sequence, optional): Secrets required by the
                pipeline execution. Those secrets must be defined in the
                launching user's account. User secrets take precedence over
                workspace secrets. Defaults to an empty list.
            workspace_secrets (Sequence, optional): Secrets required by the
                pipeline execution. Those secrets must be defined in the
                opened workspace. Defaults to an empty list.
            pre_run_script (str, optional): A Bash script that's executed
                in the same environment where Nextflow runs just before
                the pipeline is launched. Defaults to None.
            init_data (Mapping, optional): An alternate request payload for
                launching a workflow. It's recommended to generate a basic
                request using `init_launch_workflow_data()` and modifying
                it before passing it to `init_data`. Defaults to None.

        Returns:
            dict: Information about the just-launched workflow run.
        """
        endpoint = "/workflow/launch"
        params = self.init_params()
        arguments = {
            "launch": {
                "configProfiles": dedup(profiles),
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
