from functools import partial
import re
import sevenbridges as sbg
from sevenbridges.meta.transformer import Transform


ENDPOINTS = {
    "cavatica": "https://cavatica-api.sbgenomics.com/v2",
    "cgc": "https://cavatica-api.sbgenomics.com/v2",
    "sevenbridges": "https://api.sbgenomics.com/v2",
}


class SbgUtils:
    def __init__(self, client_args) -> None:
        self.client = sbg.Api(**client_args)
        self._project = None

    def extract_id(self, resource):
        resource_id = Transform.to_resource(resource)
        return resource_id

    def get_or_create(self, get_fn, create_fn):
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
        result_id = self.extract_id(result)
        return result_id

    @staticmethod
    def bundle_client_args(auth_token, platform="cavatica", endpoint=None, **kwargs):
        assert platform is None or platform in ENDPOINTS
        assert platform or endpoint
        endpoint = endpoint if endpoint else ENDPOINTS[platform]
        client_args = dict(url=endpoint, token=auth_token, **kwargs)
        return client_args

    def get_billing_group(self, billing_group_name):
        billing_groups = self.client.billing_groups.query()
        matches = [bg for bg in billing_groups if bg.name == billing_group_name]
        return matches

    def _get_project_by_id(self, project_id):
        try:
            project = self.client.projects.get(project_id)
            projects = project
        except sbg.errors.NotFound:
            projects = []
        return projects

    def _get_project_by_name(self, project_name):
        projects = self.client.projects.query(name=project_name)
        return projects

    def get_project(self, project_name=None, project_id=None):
        assert project_name or project_id  # At least one is given
        assert not (project_name and project_id)  # Both cannot be given
        if project_name:
            matches = self._get_project_by_name(project_name)
        else:
            matches = self._get_project_by_id(project_id)
        return matches

    def create_project(self, project_name, billing_group_name):
        billing_group = self.get_billing_group(billing_group_name)
        project = self.client.projects.create(project_name, billing_group)
        return project

    def get_or_create_project(self, project_name, billing_group_name):
        get_fn = partial(self.get_project, project_name)
        create_fn = partial(self.create_project, project_name, billing_group_name)
        return self.get_or_create(get_fn, create_fn)

    @property
    def project(self):
        if self._project is None:
            raise ValueError("Project not opened yet. Use `open_project()`.")
        return self._project

    def open_project(self, project):
        project_id = self.extract_id(project)
        project = self.get_project(project_id=project_id)
        self._project = project

    def get_public_app(self, app_id):
        public_apps = self.client.apps.query(visibility="public", id=app_id)
        assert len(public_apps) == 1
        public_app = public_apps[0]
        return public_app

    def _get_apps_by_name(self, app_name):
        project_apps = self.client.apps.query(project=self.project, q=app_name)
        return project_apps

    def _get_app_suffix(self, app_slug, increment=False):
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

    def get_imported_app_name(self, app_id, increment=False):
        public_app = self.get_public_app(app_id)
        app_slug = public_app.id.split("/")[-1]
        suffix = self._get_app_suffix(app_slug, increment)
        app_name = f"{app_slug}{suffix}"
        return app_name

    def get_imported_app(self, app_id):
        app_name = self.get_imported_app_name(app_id)
        apps = self.client.apps.query(project=self.project, q=app_name)
        # If multiple projects exist, pick the one with the shortest name
        apps = [x for x in apps if not x.raw.get("sbg:archived", False)]
        if len(apps) > 1:
            apps = sorted(apps, key=lambda x: len(x.id))
            apps = apps[:1]
        return apps

    def import_app(self, app_id):
        public_app = self.get_public_app(app_id)
        app_name = self.get_imported_app_name(app_id, increment=True)
        project_app = public_app.copy(project=self.project, name=app_name)
        return project_app

    def get_or_create_imported_app(self, app_id):
        get_fn = partial(self.get_imported_app, app_id)
        create_fn = partial(self.import_app, app_id)
        return self.get_or_create(get_fn, create_fn)

    def _get_volume_by_id(self, volume_id):
        try:
            volume = self.client.volumes.get(volume_id)
            volumes = [volume]
        except sbg.errors.NotFound:
            volumes = []
        return volumes

    def _get_volume_by_name(self, volume_name):
        volumes = self.client.volumes.query()
        volumes = [vol for vol in volumes if vol.name == volume_name]
        return volumes

    def get_volume(self, volume_name=None, volume_id=None):
        assert volume_name or volume_id  # At least one is given
        assert not (volume_name and volume_id)  # Both cannot be given
        if volume_name:
            matches = self._get_volume_by_name(volume_name)
        else:
            matches = self._get_volume_by_id(volume_id)
        return matches
