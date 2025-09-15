import shutil

from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.spare_jobs import spare_job_list
from lamp_py.tableau.jobs.spare_jobs import SPARE_TABLEAU_PROJECT

from lamp_py.tableau.spare.default_converter import PolarsDataFrameConverter, default_converter_from_s3
from lamp_py.tableau.spare.autogen_01_schema_printer import (
    springboard_spare_cases_with_history,
    tableau_spare_cases_with_history,
    tableau_spare_walletTransactions_with_history,
    springboard_spare_walletTransactions_with_history,
)


def start_spare_single() -> None:

    job = FilteredHyperJob(
        remote_input_location=springboard_spare_walletTransactions_with_history,
        remote_output_location=tableau_spare_walletTransactions_with_history,
        rollup_num_days=None,
        processed_schema=default_converter_from_s3(springboard_spare_walletTransactions_with_history),
        parquet_preprocess=None,
        dataframe_filter=PolarsDataFrameConverter.convert_to_tableau_flat_schema,
        parquet_filter=None,
        tableau_project_name=SPARE_TABLEAU_PROJECT,
    )
    outs = job.run_parquet(None)
    # outs1 = job.create_parquet(None)
    outs2 = job.create_local_hyper()


def start_spare() -> None:
    """Run all HyperFile Update Jobs"""
    local_parquet = False
    run_pq_remote = False
    local_hyper = False
    run_hyper_remote = False
    combined = True

    if local_parquet:
        for job in spare_job_list:
            try:
                outs = job.create_parquet(None)
                shutil.copy(job.local_parquet_path, job.local_hyper_path.replace(".hyper", ".parquet"))
            except Exception as e:
                print(f"{job.remote_parquet_path} parquet/local unable to generate - {e}")

    if run_pq_remote:
        for job in spare_job_list:
            try:
                outs = job.run_parquet(None)
            except Exception as e:
                print(f"{job.remote_parquet_path} parquet/upload unable to generate - {e}")

    if local_hyper:
        for job in spare_job_list:
            try:
                shutil.copy(job.local_hyper_path.replace(".hyper", ".parquet"), job.local_parquet_path)
                outs = job.create_local_hyper(use_local=True)
            except Exception as e:
                print(f"{job.remote_parquet_path} hyper/local unable to generate - {e}")

    if run_hyper_remote:
        for job in spare_job_list:
            try:
                outs = job.run_hyper()
            except Exception as e:
                print(f"{job.remote_parquet_path} hyper/upload unable to generate - {e}")

    if combined:
        for job in spare_job_list:
            try:
                outs = job.run_parquet_hyper_combined_job()
            except Exception as e:
                print(f"{job.remote_parquet_path} combined unable to generate - {e}")
            #


if __name__ == "__main__":
    # start_spare()
    start_spare_single()
