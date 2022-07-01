from prefect import task
from sagetasks.sevenbridges.general import (
    sbg_bundle_client_args,
    sbg_get_project_id,
    sbg_get_imported_app_id,
    sbg_get_volume_id,
)


sbg_bundle_client_args = task(
    sbg_bundle_client_args, name="SevenBridges - Bundle client arguments"
)


sbg_get_project_id = task(
    sbg_get_project_id, name="SevenBridges - Get (or create) project"
)


sbg_get_imported_app_id = task(
    sbg_get_imported_app_id,
    name="SevenBridges - Get (or import) an imported copy of a public app",
)


sbg_get_volume_id = task(sbg_get_volume_id, name="SevenBridges - Get a cloud volume")
