from copy import deepcopy
import re
import numpy as np

GTEX_NORMAL_PARAMS = {
    "alignInsertionFlush": "None",
    "alignIntronMax": 1000000,
    "alignMatesGapMax": 1000000,
    "alignSJDBoverhangMin": 1,
    "alignSJoverhangMin": 8,
    "alignSoftClipAtReferenceEnds": "Yes",
    "chimJunctionOverhangMin": 15,
    "chimMainSegmentMultNmax": 1,
    "chimSegmentMin": 15,
    "limitSjdbInsertNsj": 1200000,
    "outFilterIntronMotifs": "None",
    "outFilterMatchNminOverLread": 0.33,
    "outFilterMismatchNmax": 999,
    "outFilterMismatchNoverLmax": 0.1,
    "outFilterMultimapNmax": 20,
    "outFilterScoreMinOverLread": 0.33,
    "outFilterType": "BySJout",
    "twopassMode": "Basic",
}

STRANDEDNESS_DEFAULTS = ("default", "rf-stranded", "fr-stranded")

TUMOR_REGEX = re.compile(r"tumou?r")


def get_unique_value(df, col, default=None):
    if col not in df and default is not None:
        raw = [default]
    else:
        raw = df[col].unique()
    assert len(raw) == 1
    val = raw[0]
    if isinstance(val, np.int64):
        val = int(val)
    return val


def format_rg_val(val):
    return re.sub(r"\s", "_", val)


# TODO: Currently assumes paired-end data
# TODO: Separate function to import suggested reference files
# TODO: Improve task naming
def manifest_to_kfdrc_rnaseq_workflow_inputs_factory(
    file_col="cavatica_file_id",
    sample_col="sample_id",
    readlen_col="read_length",
    sampletype_col="sample_type",
    orientation_col="read_orientation",
    orientation_vals=("R1", "R2"),
    strandedness_col="strandedness",
    strandedness_vals=STRANDEDNESS_DEFAULTS,
    library_col="library_id",
    platform_col="platform",
):
    def manifest_to_kfdrc_rnaseq_workflow_inputs(client, manifest):
        # Unpacking valid values
        r1_val, r2_val = orientation_vals
        strandedness_map = dict(zip(strandedness_vals, STRANDEDNESS_DEFAULTS))

        # Prepare inputs
        by_sample = manifest.groupby(sample_col)
        for sample_id in by_sample.groups:
            # Subset manifest for specific sample
            sample_df = by_sample.get_group(sample_id)
            assert len(sample_df.index) == 2
            # Retrieve Cavatica file IDs
            r1_file_id = sample_df[sample_df[orientation_col] == r1_val][file_col].iat[
                0
            ]
            r2_file_id = sample_df[sample_df[orientation_col] == r2_val][file_col].iat[
                0
            ]
            # Retrieve other metadata
            strandedness_raw = get_unique_value(sample_df, strandedness_col)
            strandedness = strandedness_map.get(strandedness_raw, None)
            assert strandedness
            library_id = get_unique_value(sample_df, library_col, "Not_Reported")
            platform = get_unique_value(sample_df, platform_col, "Not_Reported")
            read_length = get_unique_value(sample_df, readlen_col)
            sample_type = get_unique_value(sample_df, sampletype_col)
            is_tumor = TUMOR_REGEX.search(sample_type)
            # Prepare params dictionary
            inputs = dict() if is_tumor else deepcopy(GTEX_NORMAL_PARAMS)
            updates = {
                "input_type": "FASTQ",
                "reads1": client.files.get(r1_file_id),
                "reads2": client.files.get(r2_file_id),
                "runThreadN": 36,
                "wf_strand_param": strandedness,
                "sample_name": sample_id,
                "rmats_read_length": read_length,
                "outSAMattrRGline": "\t".join(
                    map(
                        format_rg_val,
                        [
                            f"ID:{sample_id}",
                            f"LB:{library_id}",
                            f"PL:{platform}",
                            f"SM:{sample_id}",
                        ],
                    )
                ),
            }
            inputs.update(updates)
            task_name = "kf-rnaseq-workflow - " + sample_id

            def callback_fn(task):
                task.inputs["output_basename"] = task.id
                task.save()

            yield task_name, inputs, callback_fn

    return manifest_to_kfdrc_rnaseq_workflow_inputs
