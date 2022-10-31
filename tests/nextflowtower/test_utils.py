from copy import deepcopy

import pytest

from sagetasks.nextflowtower import utils
from sagetasks.nextflowtower.utils import TowerUtils

EG_UTILS_ARGS = {"platform": "tower.nf", "auth_token": "foobar"}
EG_WORKSPACE_ID = "foobar"


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
        tower_utils.open_workspace("barfoo")
        assert tower_utils._workspace == "barfoo"

    def test_close_workspace(self, tower_utils):
        tower_utils.close_workspace()
        assert tower_utils._workspace is None

    def test_init_params(self, tower_utils):
        params = tower_utils.init_params()
        assert params["workspaceId"] == EG_WORKSPACE_ID

    def test_extract_resource(self, tower_utils):
        multiple = {"foo": "bar", "tic": "tac"}
        single = {"thing": multiple}
        # Single element (extracted successfully)
        single_result = tower_utils.extract_resource(single)
        assert single_result == multiple
        # Multiple elements (specified key)
        multiple_key_result = tower_utils.extract_resource(multiple, "tic")
        assert multiple_key_result == "tac"
        # Multiple elements (input = output)
        multiple_result = tower_utils.extract_resource(multiple)
        assert multiple_result == multiple

    def test_bundle_client_args(self):
        result = TowerUtils.bundle_client_args(**EG_UTILS_ARGS)
        platform = EG_UTILS_ARGS["platform"]
        expected_endpoint = utils.ENDPOINTS[platform]
        expected_token = EG_UTILS_ARGS["auth_token"]
        assert result["tower_api_url"] == expected_endpoint
        assert result["tower_token"] == expected_token

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

    def test_get_compute_env(self, mocker, tower_utils):
        mocked_request = mocker.patch.object(utils.TowerClient, "request")
        mocked_request.return_value = self.EG_COMPUTE_ENV
        compute_env_id = self.EG_COMPUTE_ENV["computeEnv"]["id"]
        result = tower_utils.get_compute_env(compute_env_id)
        assert result == self.EG_COMPUTE_ENV["computeEnv"]

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

    def test_get_workflow(self, mocker, tower_utils):
        mocked_request = mocker.patch.object(utils.TowerClient, "request")
        mocked_request.return_value = self.EG_WORKFLOW
        workflow_id = self.EG_WORKFLOW["workflow"]["id"]
        result = tower_utils.get_workflow(workflow_id)
        assert result == self.EG_WORKFLOW

    EG_DICT = {
        "foo": [1, 2, 3],
        "bar": None,
        "baz": {"tic": 1.1, "tac": 2.2, "toe": 3.3},
    }

    def test_update_dict_none(self, tower_utils):
        # Expecting no change due to None value
        overrides = {"foo": None}
        result = tower_utils.update_dict(self.EG_DICT, overrides)
        assert result == self.EG_DICT

    def test_update_dict_invalid(self, tower_utils):
        # Expecting no change due to None value
        overrides = {"oof": "gah"}
        with pytest.raises(ValueError):
            tower_utils.update_dict(self.EG_DICT, overrides)

    def test_update_dict_toplevel(self, tower_utils):
        # Expecting top-level changes
        overrides = {"foo": [4, 5, 6], "bar": False}
        result = tower_utils.update_dict(self.EG_DICT, overrides)
        assert result["foo"] == overrides["foo"]
        assert result["bar"] == overrides["bar"]

    def test_update_dict_nested(self, tower_utils):
        # Expecting nested changes
        overrides = {"baz": {"tic": 7.7, "tac": {"some": "thing"}}}
        result = tower_utils.update_dict(self.EG_DICT, overrides)
        assert result["baz"]["tic"] == overrides["baz"]["tic"]
        assert result["baz"]["tac"] == overrides["baz"]["tac"]

    def test_update_dict_copy(self, tower_utils):
        # Expecting input dictionary to not change
        eg_dict_copy = deepcopy(self.EG_DICT)
        overrides = {"bar": "random"}
        result = tower_utils.update_dict(self.EG_DICT, overrides)
        assert result["bar"] == overrides["bar"]
        assert self.EG_DICT == eg_dict_copy

    def test_init_launch_workflow_data(self, mocker, tower_utils):
        compute_env = self.EG_COMPUTE_ENV["computeEnv"]
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

    EG_LAUNCH = {"workflowId": "7g2R5Z1J"}

    def test_launch_workflow(self, mocker, tower_utils):
        # Mock TowerUtils class but use static reference for `launch_workflow()`
        # Credit: https://stackoverflow.com/a/35782720
        compute_env_id = self.EG_COMPUTE_ENV["computeEnv"]["id"]
        pipeline = self.EG_WORKFLOW["workflow"]["projectName"]
        mocked_utils = mocker.MagicMock(tower_utils, autospec=True)
        # Test the function call
        TowerUtils.launch_workflow(mocked_utils, compute_env_id, pipeline)
        # Check for expected method calls
        mocked_utils.init_params.assert_called_once()
        mocked_utils.init_launch_workflow_data.assert_called_once()
        mocked_utils.update_dict.assert_called()
        mocked_utils.client.request.assert_called_once()
        mocked_utils.extract_resource.assert_called()
        mocked_utils.get_workflow.assert_called_once()
