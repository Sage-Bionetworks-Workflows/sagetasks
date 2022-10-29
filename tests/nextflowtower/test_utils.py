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
