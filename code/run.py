"""Entry point to the pyspark job."""
import os
import typer
from pathlib import Path

from src.jobs.main import jobs_main, spark_build
from src.jobs.utils.general import EnvEnum
from src.jobs.utils import log_utils

DATA_PATH = os.environ.get("DATA_PATH", f"file:///{Path(__file__).parent}/data")
CLOUD_ENV = os.environ.get("CLOUD_ENV", f"file:///{Path(__file__).parent}/output")

def main(
    env: EnvEnum = typer.Argument(..., help="Environment for the spark-job"),
    input_dir: str = typer.Argument(
        DATA_PATH, help="File which will be parsed"),
    output_dir: str = typer.Argument(CLOUD_ENV, help="File where output will be stored"),
) -> None:
    """Execute main function for the package."""
    with spark_build(env=env) as spark:
        logger = log_utils.Logger(env=env, spark=spark)
        logger.info("Spark and logger initialized")
        jobs_main(spark, logger, input_dir=input_dir, output_dir=output_dir)


if __name__ == "__main__":
    typer.run(main)
