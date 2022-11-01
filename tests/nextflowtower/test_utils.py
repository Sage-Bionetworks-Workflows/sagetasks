import pytest

from sagetasks.nextflowtower import utils
from sagetasks.nextflowtower.utils import TowerUtils
from sagetasks.utils import dedup

EG_UTILS_ARGS = {"platform": "tower.nf", "auth_token": "foobar"}

EG_WORKSPACE_ID = 123456

EG_COMPUTE_ENV = {
    "computeEnv": {
        "id": "a1b2c3",
        "name": "test-project-ce",
        "platform": "aws-batch",
        "config": {
            "workDir": "s3://test-project-tower-scratch/work",
            "preRunScript": "NXF_OPTS='-Xms4g -Xmx12g'",
            "postRunScript": None,
        },
        "status": "AVAILABLE",
        "orgId": 98765,
        "workspaceId": 65748,
    }
}

EG_WORKFLOW = {
    "workflow": {
        "id": "7g2R5Z1J",
        "runName": "tiny_shaw",
        "sessionId": "n8b7v6-o4i5-u6q8w6-e2s7l1",
        "profile": "test",
        "workDir": "s3://test-project-tower-scratch/work",
        "userName": "bgrande",
        "revision": "3.9",
        "commandLine": "nextflow run ...",
        "projectName": "nf-core/rnaseq",
        "launchId": "y1u2i3",
        "status": "SUBMITTED",
    },
    "progress": {
        "workflowProgress": {},
        "processesProgress": [],
    },
    "platform": {"id": "aws-batch", "name": "Amazon Batch"},
    "jobInfo": {},
}

EG_LAUNCH = {"workflowId": "7g2R5Z1J"}


@pytest.fixture
def tower_utils_client_args():
    return TowerUtils.bundle_client_args(**EG_UTILS_ARGS)


@pytest.fixture
def tower_utils(tower_utils_client_args):
    tower_utils = TowerUtils(tower_utils_client_args)
    tower_utils.open_workspace(EG_WORKSPACE_ID)
    return tower_utils


class TestTowerUtils:
    def test__init__(self, mocker, tower_utils_client_args):
        mocked_client = mocker.patch.object(utils, "TowerClient")
        result = TowerUtils(tower_utils_client_args)
        mocked_client.assert_called_once()
        assert result.client is mocked_client.return_value

    def test_workspace(self, tower_utils):
        assert tower_utils.workspace == EG_WORKSPACE_ID
        tower_utils.close_workspace()
        with pytest.raises(ValueError):
            tower_utils.workspace

    def test_open_workspace(self, tower_utils):
        tower_utils.open_workspace(987654)
        assert tower_utils._workspace == 987654

    def test_close_workspace(self, tower_utils):
        tower_utils.close_workspace()
        assert tower_utils._workspace is None

    def test_init_params(self, tower_utils):
        params = tower_utils.init_params()
        assert params["workspaceId"] == EG_WORKSPACE_ID

    def test_bundle_client_args(self):
        result = TowerUtils.bundle_client_args(**EG_UTILS_ARGS)
        platform = EG_UTILS_ARGS["platform"]
        expected_endpoint = utils.ENDPOINTS[platform]
        expected_token = EG_UTILS_ARGS["auth_token"]
        assert result["tower_api_url"] == expected_endpoint
        assert result["tower_token"] == expected_token

    def test_get_compute_env(self, mocker, tower_utils):
        mocked_request = mocker.patch.object(utils.TowerClient, "request")
        mocked_request.return_value = EG_COMPUTE_ENV
        compute_env_id = EG_COMPUTE_ENV["computeEnv"]["id"]
        result = tower_utils.get_compute_env(compute_env_id)
        assert result == EG_COMPUTE_ENV["computeEnv"]

    def test_get_workflow(self, mocker, tower_utils):
        mocked_request = mocker.patch.object(utils.TowerClient, "request")
        mocked_request.return_value = EG_WORKFLOW
        workflow_id = EG_WORKFLOW["workflow"]["id"]
        result = tower_utils.get_workflow(workflow_id)
        assert result == EG_WORKFLOW

    def test_init_launch_workflow_data(self, mocker, tower_utils):
        compute_env = EG_COMPUTE_ENV["computeEnv"]
        compute_env_id = compute_env["id"]
        ce_config = compute_env["config"]
        mocked = mocker.patch.object(tower_utils, "get_compute_env")
        mocked.return_value = compute_env
        result = tower_utils.init_launch_workflow_data(compute_env_id)
        launch = result["launch"]
        assert launch["postRunScript"] == ce_config["postRunScript"]
        assert launch["preRunScript"] == ce_config["preRunScript"]
        assert launch["workDir"] == ce_config["workDir"]
        assert launch["dateCreated"] is not None

    @pytest.mark.parametrize(
        "args",
        [
            {"compute_env_id": "a1b2c4", "pipeline": "sage/work"},
            {"compute_env_id": "a1b2c4", "pipeline": "sage/work", "run_name": "test"},
            {
                "compute_env_id": "a1b2c4",
                "pipeline": "sage/work",
                "user_secrets": ["test", "test"],
            },
        ],
    )
    def test_launch_workflow(self, mocker, tower_utils, args):
        # Mock TowerUtils class but use static reference for `launch_workflow()`
        # Credit: https://stackoverflow.com/a/35782720
        mocked_update_dict = mocker.patch.object(utils, "update_dict")
        mocked_utils = mocker.MagicMock(tower_utils, autospec=True)
        mocked_utils.client.request.return_value = EG_LAUNCH
        # Test the function call
        TowerUtils.launch_workflow(mocked_utils, **args)
        # Check for expected method calls
        mocked_utils.init_params.assert_called_once()
        mocked_utils.init_launch_workflow_data.assert_called_once_with(
            args["compute_env_id"]
        )
        mocked_update_dict.assert_called_once()
        mocked_utils.client.request.assert_called_once()
        mocked_utils.get_workflow.assert_called_once_with(EG_LAUNCH["workflowId"])
        # Check for values
        (init_data, arguments), _ = mocked_update_dict.call_args
        launch_args = arguments["launch"]
        assert init_data == mocked_utils.init_launch_workflow_data.return_value
        assert launch_args["pipeline"] == args["pipeline"]
        assert launch_args["runName"] == args.get("run_name")
        assert launch_args["userSecrets"] == dedup(args.get("user_secrets", []))
