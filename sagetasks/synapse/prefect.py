from prefect import task
from sagetasks.synapse.general import (
    syn_bundle_client_args,
    syn_get_dataframe,
    syn_store_dataframe,
)


syn_bundle_client_args = task(
    syn_bundle_client_args, name="Synapse - Bundle client arguments"
)


syn_get_dataframe = task(
    syn_get_dataframe, name="Synapse - Download and load data frame"
)


syn_store_dataframe = task(
    syn_store_dataframe, name="Synapse - Serialize and upload data frame"
)
