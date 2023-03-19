"""This module is the entry-point for the run.py to handle spark session \
building and ETL."""

import contextlib
from pathlib import Path
from typing import Generator

from pyspark.sql import SparkSession

from src.jobs.extract import extract_file
from src.jobs.load import write_to_path
from src.jobs.transform import get_feature_set
from src.jobs.utils.general import EnvEnum
from src.jobs.utils.log_utils import Logger


def jobs_main(spark: SparkSession, logger: Logger, input_dir: str, output_dir: str) -> None:
    """
    High-level function to perform the ETL job.

    Args:
        spark (SparkSession) : spark session to perform ETL job
        logger (Logger) : logger class instance
        input_dir (str): path on which the job will be performed
        output_dir (str): path where output will be stored
    """
    df_match = extract_file(spark, input_dir, "match.csv")
    df_player_attributes = extract_file(spark, input_dir, "player_attributes.csv")
    logger.info(f"{input_dir} extracted to DataFrame")

    feature_df = get_feature_set(df_match, df_player_attributes)

    write_to_path(feature_df, output_dir)
    logger.info("Written features dataframe to path")


@contextlib.contextmanager
def spark_build(env: EnvEnum) -> Generator[SparkSession, None, None]:
    """
    Build the spark object.

    Args:
        env (EnvEnum): environment of the spark-application

    Yields:
        SparkSession object

    """
    spark_builder = SparkSession.builder
    app_name = Path(__file__).parent.name

    if env == EnvEnum.dev:
        spark = spark_builder.appName(app_name).getOrCreate()
    else:
        raise NotImplementedError
    try:
        yield spark
    finally:
        spark.stop()
