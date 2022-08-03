import os

import docker


def bundle_client_args(auth_token, docker_registry="docker.io", **kwargs):
    """Docker - Bundle client arguments"""
    client_args = dict(
        username="foo", password=auth_token, registry=docker_registry
    )
    client_args.update(kwargs)
    return client_args


def remove_docker_container(client_args, container_name):
    """Remove docker container"""
    client = docker.from_env()
    client.login(**client_args)
    try:
        cont = client.containers.get(container_name)
        cont.stop()
        cont.remove()
    except Exception:
        print("Unable to remove container")


def remove_docker_image(client_args, image_name):
    """Remove docker image"""
    client = docker.from_env()
    client.login(**client_args)
    try:
        client.images.remove(image_name, force=True)
    except Exception:
        print("Unable to remove image")


def docker_run(client_args, docker_repository, input_dir, container_name):
    """Docker run"""
    client = docker.from_env()
    client.login(**client_args)

    # docker_image = args.docker_repository + "@" + args.docker_digest
    docker_image = docker_repository

    # These are the volumes that you want to mount onto your docker container
    output_dir = os.path.join(os.getcwd(), "output")
    # Must make the directory or else it will be mounted into docker as a file
    os.mkdir(output_dir)

    print("mounting volumes")
    # These are the locations on the docker that you want your mounted
    # volumes to be + permissions in docker (ro, rw)
    # It has to be in this format '/output:rw'
    mounted_volumes = {output_dir: '/output:rw',
                       input_dir: '/input:ro'}
    # All mounted volumes here in a list
    all_volumes = [output_dir, input_dir]
    # Mount volumes
    volumes = {}
    for vol in all_volumes:
        volumes[vol] = {'bind': mounted_volumes[vol].split(":")[0],
                        'mode': mounted_volumes[vol].split(":")[1]}

    # Look for if the container exists already, if so, reconnect
    print("checking for containers")
    container = None
    for cont in client.containers.list(all=True):
        if container_name in cont.name:
            # Must remove container if the container wasn't killed properly
            if cont.status == "exited":
                cont.remove()
            else:
                container = cont
    # If the container doesn't exist, make sure to run the docker image
    if container is None:
        # Run as detached, logs will stream below
        print("running container")
        container = client.containers.run(
            docker_image,
            detach=True, volumes=volumes,
            name=container_name,
            network_disabled=True,
            mem_limit='10g', stderr=True
        )