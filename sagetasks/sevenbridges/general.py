from sagetasks.sevenbridges.utils import SbgUtils


def sbg_bundle_client_args(auth_token, platform="cavatica", endpoint=None, **kwargs):
    return SbgUtils.bundle_client_args(auth_token, platform, endpoint, **kwargs)


def sbg_get_project_id(client_args, project_name, billing_group_name):
    utils = SbgUtils(client_args)
    project = utils.get_or_create_project(project_name, billing_group_name)
    project_id = utils.extract_id(project)
    return project_id


def sbg_get_imported_app_id(client_args, project, app_id=None):
    utils = SbgUtils(client_args)
    utils.open_project(project)
    imported_app = utils.get_or_create_imported_app(app_id)
    imported_app_id = utils.extract_id(imported_app)
    return imported_app_id


def sbg_get_volume_id(client_args, volume_name=None, volume_id=None):
    utils = SbgUtils(client_args)
    volumes = utils.get_volume(volume_name, volume_id)
    assert len(volumes) > 0, "This function cannot create a volume if it's missing"
    assert len(volumes) < 2, "Use a more specific volume name or use an ID instead"
    volume = volumes[0]
    volume_id = utils.extract_id(volume)
    return volume_id
