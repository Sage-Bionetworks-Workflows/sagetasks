"""Collection of Seven Bridges Genomics-related Prefect tasks

Adapted from code that Tom Yu authored:
https://github.com/include-dcc/synapse-cavatica/blob/main/scripts/rnaseq_flow.py
"""

import time
from prefect import Task
import sevenbridges as sbg


ENDPOINTS = {
    "cavatica": "https://cavatica-api.sbgenomics.com/v2",
    "cgc": "https://cavatica-api.sbgenomics.com/v2",
    "sevenbridges": "https://api.sbgenomics.com/v2",
}


class SbgBaseTask(Task):
    def get_client(self, client_args):
        return sbg.Api(**client_args)

    def purge_api(self, obj):
        if hasattr(obj, "_API"):
            obj._API = None
        if hasattr(obj, "_api"):
            obj._api = None
        if hasattr(obj, "_data") and hasattr(obj._data, "api"):
            obj._data.api = None
        return obj

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
        result = self.purge_api(result)
        return result

    def run(self):
        raise NotImplementedError("The `run` method isn't implemented.")


class SbgClientArgsTask(SbgBaseTask):
    def run(self, auth_token, platform="cavatica", endpoint=None, **kwargs):
        assert platform is None or platform in ENDPOINTS
        assert platform or endpoint
        endpoint = endpoint if endpoint else ENDPOINTS[platform]
        client_args = dict(url=endpoint, token=auth_token, **kwargs)
        return client_args


class SbgGetProjectTask(SbgBaseTask):
    @staticmethod
    def get_fn(client, project_name):
        def get():
            return client.projects.query(name=project_name)

        return get

    @staticmethod
    def create_fn(client, project_name, billing_group_id):
        def create():
            bg = client.billing_groups.get(id=billing_group_id)
            client.projects.create(name=project_name, billing_group=bg)

        return create

    def run(self, client_args, project_name, billing_group_id):
        client = self.get_client(client_args)
        get_fn = self.get_fn(client, project_name)
        create_fn = self.create_fn(client, project_name, billing_group_id)
        return self.get_or_create(get_fn, create_fn)


class SbgGetAppTask(SbgBaseTask):
    @staticmethod
    def get_fn(client, project, app_name):
        def get():
            project_apps = client.apps.query(project=project.id, q=app_name)
            project_apps = sorted(project_apps, key=lambda x: len(x.id))
            if len(project_apps) > 1:
                project_apps = project_apps[:1]
            return project_apps

        return get

    @staticmethod
    def create_fn(client, project, app_id, app_name):
        def create():
            # Retrieve public application
            public_apps = client.apps.query(visibility="public", id=app_id)
            assert len(public_apps) == 1
            public_app = public_apps[0]
            # Copy public application
            public_app.copy(project=project.id, name=app_name)

        return create

    def run(self, client_args, app_id, project):
        client = self.get_client(client_args)
        app_name = app_id.split("/")[-1]
        get_fn = self.get_fn(client, project, app_name)
        create_fn = self.create_fn(client, project, app_id, app_name)
        return self.get_or_create(get_fn, create_fn)


class SbgGetVolumeTask(SbgBaseTask):
    @staticmethod
    def get_fn(client, id=None, name=None):
        assert id or name  # At least one is given
        assert not (id and name)  # Both cannot be given

        def get_by_id():
            try:
                volume = client.volumes.get(id=id)
                volumes = [volume]
            except sbg.errors.NotFound:
                volumes = []
            return volumes

        def get_by_name():
            volumes = client.volumes.query()
            volumes = [vol for vol in volumes if vol.name == name]
            return volumes

        if id:
            return get_by_id
        elif name:
            return get_by_name
        else:
            raise NotImplementedError("The `get` method isn't implemented.")

    @staticmethod
    def create_fn():
        def create():
            # Not implemented because we expect these volumes to be automatically
            # configured when the Cavatica project is created by Data Tracker
            raise NotImplementedError("The `create` method isn't implemented.")

        return create

    def run(self, client_args, id=None, name=None):
        client = self.get_client(client_args)
        get_fn = self.get_fn(client, id, name)
        create_fn = self.create_fn()
        return self.get_or_create(get_fn, create_fn)
