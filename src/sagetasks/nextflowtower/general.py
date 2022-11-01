from sagetasks.nextflowtower.utils import TowerUtils


def bundle_client_args(auth_token, platform="tower.nf", endpoint=None, **kwargs):
    """Nextflow Tower - Bundle client arguments"""
    return TowerUtils.bundle_client_args(auth_token, platform, endpoint, **kwargs)
