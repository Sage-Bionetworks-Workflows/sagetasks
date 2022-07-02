from prefect import task
import sagetasks.sevenbridges.general as gen


bundle_client_args = task(
    gen.bundle_client_args, name="SevenBridges - Bundle client arguments"
)


get_project_id = task(gen.get_project_id, name="SevenBridges - Get (or create) project")


get_copied_app_id = task(
    gen.get_copied_app_id,
    name="SevenBridges - Get (or import) an imported copy of a public app",
)


get_volume_id = task(gen.get_volume_id, name="SevenBridges - Get a cloud volume")


import_volume_file = task(
    gen.import_volume_file, name="SevenBridges - Import a file from a volume"
)
