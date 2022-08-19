import re
import time
from functools import partial
from pathlib import PurePosixPath

import sevenbridges as sbg
from sevenbridges import ImportExportState
from sevenbridges.http.error_handlers import maintenance_sleeper, rate_limit_sleeper
from sevenbridges.meta.transformer import Transform
from sevenbridges.models.project import Project

ENDPOINTS = {
    "cavatica": "https://cavatica-api.sbgenomics.com/v2",
    "cgc": "https://cavatica-api.sbgenomics.com/v2",
    "sevenbridges": "https://api.sbgenomics.com/v2",
}


class SbgUtils:
    def __init__(self, client_args) -> None:
        """Initializes the SevenBridges client with the bundled information.

        `client_args` can be generated with the `bundle_client_args()` static method.

        Optionally, you can set a default project for various methods with the
        `open_project()` method.
        """
        self.client = sbg.Api(
            **client_args, error_handlers=[rate_limit_sleeper, maintenance_sleeper]
        )
        self._project = None

    def extract_id(self, resource):
        """Extracts the resource ID (or returns the ID if already a string).

        These IDs are strings, which can be pickled unlike the resource objects.
        """
        resource_id = Transform.to_resource(resource)
        return resource_id

    def get_or_create(self, get_fn, create_fn):
        """Gets a single resource or creates it if missing."""
        collection = get_fn()
        if len(collection) == 0:
            create_fn()
            collection = get_fn()
            assert len(collection) == 1
            result = collection[0]
        elif len(collection) == 1:
            result = collection[0]
        else:
            raise ValueError("There shouldn't be more than one match.")
        return result

    @staticmethod
    def bundle_client_args(auth_token, platform="cavatica", endpoint=None, **kwargs):
        """Bundles the information for authenticating a SevenBridges client."""
        assert platform is None or platform in ENDPOINTS
        assert platform or endpoint
        endpoint = endpoint if endpoint else ENDPOINTS[platform]
        client_args = dict(url=endpoint, token=auth_token, **kwargs)
        return client_args

    def get_billing_group(self, billing_group_name):
        """Retrieves the billing groups with the given name."""
        billing_groups = self.client.billing_groups.query()
        matches = [bg for bg in billing_groups if bg.name == billing_group_name]
        return matches

    def _get_project_by_id(self, project_id):
        """Retrieves the project with the given ID."""
        try:
            project = self.client.projects.get(project_id)
            projects = project
        except sbg.errors.NotFound:
            projects = []
        return projects

    def _get_project_by_name(self, project_name):
        """Retrieves the projects with the given name."""
        projects = self.client.projects.query(name=project_name)
        return projects

    def get_project(self, project_name=None, project_id=None):
        """Retrieves the projects with the given name or ID."""
        assert project_name or project_id  # At least one is given
        assert not (project_name and project_id)  # Both cannot be given
        if project_name:
            matches = self._get_project_by_name(project_name)
        else:
            matches = self._get_project_by_id(project_id)
        return matches

    def create_project(self, project_name, billing_group_name):
        """Creates a project with the given name and billing group."""
        billing_group = self.get_billing_group(billing_group_name)
        project = self.client.projects.create(project_name, billing_group)
        return project

    def get_or_create_project(self, project_name, billing_group_name):
        """Gets (or creates) a project with the given information."""
        get_fn = partial(self.get_project, project_name)
        create_fn = partial(self.create_project, project_name, billing_group_name)
        return self.get_or_create(get_fn, create_fn)

    @property
    def project(self):
        """Property for raising an error if a project hasn't been opened yet."""
        if self._project is None:
            raise ValueError("Project not opened yet. Use `open_project()`.")
        return self._project

    def open_project(self, project):
        """Sets the given project as the default for other methods."""
        project_id = self.extract_id(project)
        project = self.get_project(project_id=project_id)
        self._project = project

    def get_public_app(self, app_id):
        """Retrieves the public app with the given ID."""
        public_apps = self.client.apps.query(visibility="public", id=app_id)
        assert len(public_apps) == 1
        public_app = public_apps[0]
        return public_app

    def _get_apps_by_name(self, app_name):
        """Retrieves the private apps with the given name."""
        project_apps = self.client.apps.query(project=self.project, q=app_name)
        return project_apps

    def _get_app_suffix(self, app_slug, increment=False):
        """Generates an incrementing suffix for public apps that are copied."""
        apps = self._get_apps_by_name(app_slug)
        regex = re.compile(app_slug + r"-(\d)+")
        matches = [regex.search(x.id) for x in apps]
        matches = [x for x in matches if x]
        if matches:
            versions = [int(x[1]) for x in matches]
            versions.sort()
            last_version = versions[-1]
            if increment:
                last_version += 1
            suffix = f"-{last_version}"
        else:
            suffix = ""
            if increment:
                suffix = "-1"
        return suffix

    def get_copied_app_name(self, app_id, increment=False):
        """Generates a consistent naming scheme for public apps that are copied."""
        public_app = self.get_public_app(app_id)
        app_slug = public_app.id.split("/")[-1]
        suffix = self._get_app_suffix(app_slug, increment)
        app_name = f"{app_slug}{suffix}"
        return app_name

    def get_copied_app(self, app_id):
        """Retrieves a public app that's been copied to a project."""
        app_name = self.get_copied_app_name(app_id)
        apps = self.client.apps.query(project=self.project, q=app_name)
        # If multiple projects exist, pick the one with the shortest name
        apps = [x for x in apps if not x.raw.get("sbg:archived", False)]
        if len(apps) > 1:
            apps = sorted(apps, key=lambda x: len(x.id))
            apps = apps[:1]
        return apps

    def import_app(self, app_id):
        """Copies a public app into the opened project."""
        public_app = self.get_public_app(app_id)
        app_name = self.get_copied_app_name(app_id, increment=True)
        project_app = public_app.copy(project=self.project, name=app_name)
        return project_app

    def get_or_create_copied_app(self, app_id):
        """Gets (or copies) a public app in the opened project."""
        get_fn = partial(self.get_copied_app, app_id)
        create_fn = partial(self.import_app, app_id)
        return self.get_or_create(get_fn, create_fn)

    def _get_volume_by_id(self, volume_id):
        """Retrieves the cloud volume with the given ID."""
        try:
            volume = self.client.volumes.get(volume_id)
            volumes = [volume]
        except sbg.errors.NotFound:
            volumes = []
        return volumes

    def _get_volume_by_name(self, volume_name):
        """Retrieves the cloud volumes with the given name."""
        volumes = self.client.volumes.query()
        volumes = [vol for vol in volumes if vol.name == volume_name]
        return volumes

    def get_volume(self, volume_name=None, volume_id=None):
        """Retrieves the cloud volumes with the given name or ID."""
        assert volume_name or volume_id  # At least one is given
        assert not (volume_name and volume_id)  # Both cannot be given
        if volume_name:
            matches = self._get_volume_by_name(volume_name)
        else:
            matches = self._get_volume_by_id(volume_id)
        return matches

    def _get_parent_args(self, parent):
        """Generates relevant function kwargs for projects or folders."""
        if isinstance(parent, Project):
            parent_args = {"project": parent}
        else:
            parent_args = {"parent": parent}
        return parent_args

    def get_folder(self, folder_name, parent):
        """Retrieves the folder with the given name and parent."""
        parent_args = self._get_parent_args(parent)
        children = self.client.files.query(**parent_args)
        folders = [x for x in children if getattr(x, "type", None) == "folder"]
        matches = [x for x in folders if x.name == folder_name]
        return matches

    def create_folder(self, folder_name, parent):
        """Creates a folder with the given name and parent."""
        parent_args = self._get_parent_args(parent)
        folder = self.client.files.create_folder(name=folder_name, **parent_args)
        return folder

    def get_or_create_folder(self, folder_name, parent):
        """Gets (or creates) a folder with the given name and parent."""
        get_fn = partial(self.get_folder, folder_name, parent)
        create_fn = partial(self.create_folder, folder_name, parent)
        return self.get_or_create(get_fn, create_fn)

    def get_folders_recursively(self, folder_names, parent=None):
        """Gets (or creates) a nested list of folders recursively."""
        if parent is None:
            parent = self.project
        for folder_name in folder_names:
            folder = self.get_or_create_folder(folder_name, parent)
            parent = folder
        return folder

    def get_file(self, file_name, parent):
        """Retrieves a file with the given name and parent."""
        parent_args = self._get_parent_args(parent)
        children = self.client.files.query(**parent_args)
        files = [x for x in children if getattr(x, "type", None) == "file"]
        matches = [x for x in files if x.name == file_name]
        return matches

    def _wait_for_import_job(self, import_job):
        """Waits for the volume file import job to complete (successfully or not)."""
        while True:
            import_state = import_job.reload().state
            if import_state in (ImportExportState.COMPLETED, ImportExportState.FAILED):
                break
            time.sleep(5)
        return import_job

    def _get_imported_file(self, import_job):
        """Retrieves the imported file with the given import job."""
        if import_job.state == ImportExportState.COMPLETED:
            imported_file = import_job.result
        elif import_job.state == ImportExportState.FAILED:
            raise sbg.SbgError("Failed to import file from volume")
        else:
            raise sbg.SbgError("Import job not complete yet")
        return imported_file

    def import_volume_file(self, volume_id, volume_path, parent):
        """Imports the given volume file under the given parent."""
        import_job = self.client.imports.submit_import(
            volume=volume_id, parent=parent, location=volume_path
        )
        import_job = self._wait_for_import_job(import_job)
        imported_file = self._get_imported_file(import_job)
        return imported_file

    def get_or_create_volume_file(self, volume_id, volume_path, project_path):
        """Gets (or imports) the given volume file under the given project path."""
        project_path = PurePosixPath(project_path)
        dir_name = project_path.parent
        file_name = project_path.name
        folder_names = dir_name.parts
        parent = self.get_folders_recursively(folder_names)
        get_fn = partial(self.get_file, file_name, parent)
        create_fn = partial(self.import_volume_file, volume_id, volume_path, parent)
        return self.get_or_create(get_fn, create_fn)

    def get_task(self, task_name=None, app_id=None):
        """Retrieves the tasks with the given name and/or app ID."""
        matches = self.client.tasks.query(project=self.project)
        if task_name:
            matches = [t for t in matches if t.name == task_name]
        if app_id:
            matches = [t for t in matches if app_id in t.app]
        return matches

    def create_task(self, app_id, inputs, task_name, callback_fn=None):
        """Drafts a task with the given app, inputs, and task name.

        The optional `callback_fn` provides the option to update the task
        once it's created (e.g., updating an input using the task ID).
        """
        task = self.client.tasks.create(
            name=task_name, project=self.project, app=app_id, inputs=inputs, run=False
        )
        if callback_fn:
            callback_fn(task)
        return task

    def get_or_create_task(self, app_id, inputs, task_name, callback_fn=None):
        """Gets (or drafts) a task with the given app, inputs, and task name."""
        get_fn = partial(self.get_task, task_name, app_id)
        create_fn = partial(self.create_task, app_id, inputs, task_name, callback_fn)
        return self.get_or_create(get_fn, create_fn)
