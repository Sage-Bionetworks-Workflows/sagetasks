import pytest

from sagetasks.nextflowtower import utils

EG_UTILS_ARGS = {"platform": "tower.nf", "auth_token": "foobar"}
EG_WORKSPACE_ID = "foobar"


@pytest.fixture
def tower_utils_client_args():
    return utils.TowerUtils.bundle_client_args(**EG_UTILS_ARGS)


@pytest.fixture
def tower_utils(tower_utils_client_args):
    tower_utils = utils.TowerUtils(tower_utils_client_args)
    tower_utils.open_workspace(EG_WORKSPACE_ID)
    return tower_utils


class TestTowerUtils:
    def test__init__(self, mocker, tower_utils_client_args):
        mocked_client = mocker.patch.object(utils, "TowerClient")
        result = utils.TowerUtils(tower_utils_client_args)
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
        result = utils.TowerUtils.bundle_client_args(**EG_UTILS_ARGS)
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
        workflow_id = self.EG_COMPUTE_ENV["computeEnv"]["id"]
        result = tower_utils.get_compute_env(workflow_id)
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

    EG_LAUNCH = {"workflowId": "h2j3k4"}

    def test_launch_workflow(self):
        pass
