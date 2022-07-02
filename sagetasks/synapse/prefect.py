from prefect import task
import sagetasks.synapse.general as gen


bundle_client_args = task(
    gen.bundle_client_args, name="Synapse - Bundle client arguments"
)


get_dataframe = task(gen.get_dataframe, name="Synapse - Download and load data frame")


store_dataframe = task(
    gen.store_dataframe, name="Synapse - Serialize and upload data frame"
)
