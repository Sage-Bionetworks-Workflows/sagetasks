"""Collection of Seven Bridges Genomics-related Prefect tasks

Adapted from code that Tom Yu authored:
https://github.com/include-dcc/synapse-cavatica/blob/main/scripts/rnaseq_flow.py
"""

from prefect import Task
import sevenbridges as sbg


ENDPOINTS = {
    "cavatica": "https://cavatica-api.sbgenomics.com/v2",
    "cgc": "https://cavatica-api.sbgenomics.com/v2",
    "sevenbridges": "https://api.sbgenomics.com/v2",
}


class SbgBaseTask(Task):
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
        return result

    def run(self):
        raise NotImplementedError("The `run` method hasn't implemented.")


class SbgClientTask(SbgBaseTask):
    def run(self, auth_token, platform="cavatica", endpoint=None):
        assert platform is None or platform in ENDPOINTS
        assert platform or endpoint
        endpoint = endpoint if endpoint else ENDPOINTS[platform]
        return sbg.Api(url=endpoint, token=auth_token)


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

    def run(self, client, project_name, billing_group_id):
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

    def run(self, client, app_id, project):
        app_name = app_id.split("/")[-1]
        get_fn = self.get_fn(client, project, app_name)
        create_fn = self.create_fn(client, project, app_id, app_name)
        return self.get_or_create(get_fn, create_fn)
