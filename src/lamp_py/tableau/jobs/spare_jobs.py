from lamp_py.tableau.spare.default_converter import convert_to_tableau_flat_schema, default_converter_from_s3
from lamp_py.tableau.spare.autogen_01_schema_printer import spare_resources
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

SPARE_TABLEAU_PROJECT = "Spare"

spare_job_list = []

# generically create jobs
for resource, (springboard_input, tableau_output) in spare_resources.items():

    print(f"Creating Job: {resource}")
    try:
        spare_job_list.append(
            FilteredHyperJob(
                remote_input_location=springboard_input,
                remote_output_location=tableau_output,
                rollup_num_days=None,
                processed_schema=default_converter_from_s3(springboard_input),
                parquet_preprocess=None,
                dataframe_filter=convert_to_tableau_flat_schema,
                parquet_filter=None,
                tableau_project_name=SPARE_TABLEAU_PROJECT,
            )
        )
    except Exception as e:
        print(f"Could not Create Job: {resource} - {e}")
