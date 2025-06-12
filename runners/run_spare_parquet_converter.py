from lamp_py.tableau.jobs.spare import spare_job


# don't run this in pytest - environment variables in pyproject.toml point to local SPRINGBOARD/ARCHIVE
# need the .env values to run
def start_spare_parquet_updates() -> None:
    """Run all Glides Parquet Update jobs"""

    spare_job.run_parquet(None)
    # outs = spare_job.create_local_hyper()
    # print(outs)
    spare_job.run_hyper()


if __name__ == "__main__":
    start_spare_parquet_updates()
