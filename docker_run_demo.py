import os

from prefect import flow
from prefect.blocks.system import Secret

import sagetasks.docker.prefect as docker_p


@flow
def populate_secrets():
    synapse_secret = Secret(value=os.environ["SYNAPSE_AUTH_TOKEN"])
    synapse_secret.save(name="synapse-auth-token", overwrite=True)


@flow
def challenge_flow(docker_repository, input_dir, container_name):
    syn_token = Secret.load("synapse-auth-token").get()
    # Client arguments
    docker_args = docker_p.bundle_client_args(syn_token, docker_registry="docker.synapse.org")

    docker_p.docker_run(
        client_args=docker_args,
        docker_repository=docker_repository,
        input_dir=input_dir,
        container_name=container_name
    )
    docker_p.remove_docker_container(
        client_args=docker_args, container_name=container_name
    )
    docker_p.remove_docker_image(
        client_args=docker_args, image_name=docker_repository
    )


if __name__ == "__main__":
    params = {
        "docker_repository": "docker.synapse.org/syn4990358/challengeworkflowexample:valid",
        "input_dir": "/Users/tyu/sage/sagetasks/sagetasks",
        "container_name": "test_me_now",
    }
    populate_secrets()
    challenge_flow(**params)
