"""Collection of Cavatica-related Prefect tasks

Adapted from code that Tom Yu authored:
https://github.com/include-dcc/synapse-cavatica/blob/main/scripts/rnaseq_flow.py
"""

import sevenbridges as sbg

from prefect import Task


class CavaticaBaseTask(Task):

    def __init__(self, endpoint, **kwargs):
        self.endpoint = endpoint
        super().__init__(**kwargs)

    def login(self, auth_token):
        return sbg.Api(url=self.endpoint, token=auth_token)

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

    def run(self, auth_token):
        api = self.login(auth_token)
        raise NotImplementedError("The `run` method hasn't implemented.")


class CavaticaGetProjectTask(CavaticaBaseTask):

    def run(self, auth_token, project_name, billing_group_id):
        api = self.login(auth_token)

        get = lambda: api.projects.query(name=project_name)

        def create():
            bg = api.billing_groups.get(id=billing_group_id)
            api.projects.create(name=project_name, billing_group=bg)

        return self.get_or_create(get, create)

class CavaticaGetAppTask(CavaticaBaseTask):

    def run(self, auth_token, app_id, project):
        api = self.login(auth_token)
        app_name = app_id.split("/")[-1]

        def get():
            project_apps = api.apps.query(project=project.id, q=app_name)
            project_apps = sorted(project_apps, key=lambda x: len(x.id))
            if len(project_apps) > 1:
                project_apps = project_apps[:1]
            return project_apps

        def create():
            # Retrieve public application
            public_apps = api.apps.query(visibility='public', id=app_id)
            assert len(public_apps) == 1
            public_app = public_apps[0]
            # Copy public application
            public_app.copy(project=project.id, name=app_name)

        return self.get_or_create(get, create)
