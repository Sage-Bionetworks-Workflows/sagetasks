"""Collection of Cavatica-related Prefect tasks

Adapted from code that Tom Yu authored:
https://github.com/include-dcc/synapse-cavatica/blob/main/scripts/rnaseq_flow.py
"""

from prefect import Task
import sevenbridges as sbg


class CavaticaBaseTask(Task):
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


class CavaticaClientTask(CavaticaBaseTask):
    def run(self, endpoint, auth_token):
        return sbg.Api(url=endpoint, token=auth_token)


class CavaticaGetProjectTask(CavaticaBaseTask):
    def run(self, client, project_name, billing_group_id):

        get = lambda: client.projects.query(name=project_name)

        def create():
            bg = client.billing_groups.get(id=billing_group_id)
            client.projects.create(name=project_name, billing_group=bg)

        return self.get_or_create(get, create)


class CavaticaGetAppTask(CavaticaBaseTask):
    def run(self, client, app_id, project):
        app_name = app_id.split("/")[-1]

        def get():
            project_apps = client.apps.query(project=project.id, q=app_name)
            project_apps = sorted(project_apps, key=lambda x: len(x.id))
            if len(project_apps) > 1:
                project_apps = project_apps[:1]
            return project_apps

        def create():
            # Retrieve public application
            public_apps = client.apps.query(visibility="public", id=app_id)
            assert len(public_apps) == 1
            public_app = public_apps[0]
            # Copy public application
            public_app.copy(project=project.id, name=app_name)

        return self.get_or_create(get, create)
