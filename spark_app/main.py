import logging
import sys
import argparse
from spark_app.utils.logger_config import setup_logger
from pyspark.sql import SparkSession
from spark_app.utils.exceptions import JobExecutionError
from contextlib import contextmanager
from spark_app.config.settings import DEPLOY_MODE, SPARK_APP_NAME, SPARK_PACKAGE, TIMEZONE
from spark_app.jobs.flights_cdc import flight_cdc
from spark_app.jobs.customers_cdc import customer_cdc
from spark_app.jobs.bookings_cdc import booking_cdc



JOB_REGISTRY = {'flights': flight_cdc,
                'customers': customer_cdc,
                'bookings': booking_cdc
                }
AVAILABLE_JOBS = ['flights','customers','bookings']



def run_job(job,spark):
    """
    Runs a specified job using the given Spark session.  Executes the corresponding job logic,
    handles exceptions during execution, and provides logs for debugging and
    progress tracking.

    :param job: The name of the job to be executed, which is registered in the
        job registry.
    :type job: Str
    :param spark: The Spark session object used for executing the job's logic.
    :type spark: pyspark.sql.SparkSession
    :return: None
    """
    def log_exception(job_):
        logger.error(f"Error occurred while running job: {job_}")
        logger.exception('Full stack Trace:')
    try:
        logger.info(f"Running job: {job}")
        JOB_REGISTRY[job](spark)
        logger.info(f"Job: {job} completed successfully.")
    except JobExecutionError:
        log_exception(job)
        raise
    except Exception:
        log_exception(job)
        raise



def arg_parser() -> argparse.Namespace:
    """
    Parses command-line arguments for a Spark job and distinguishes between job-specific
    arguments and Spark-specific arguments. It uses `argparse` to create a parser and
    extracts the `--job` argument required for selecting and running a specific job.

    :raises SystemExit: If the required `--job` argument is missing or invalid.

    :returns: An instance of: class:`argparse.Namespace` containing parsed arguments.
    """
    script_args = []
    spark_args = []
    for arg in sys.argv:
        if arg.startswith("--job") or arg in AVAILABLE_JOBS:
            script_args.append(arg)
        else:
            spark_args.append(arg)
    parser = argparse.ArgumentParser(description='Spark Job to run')
    parser.add_argument('--job', type=str, required=True,choices=AVAILABLE_JOBS,help='Job to run. Usage: script.py --job <job_name>')
    return parser.parse_args(script_args)


@contextmanager
def start_spark_session() -> SparkSession:
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.submit.deployMode", DEPLOY_MODE) \
            .config("spark.sql.session.timeZone", TIMEZONE) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
                    "com.amazonaws.auth.profile.ProfileCredentialsProvider,"
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider,"
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()
        logger.info("Spark session started successfully..")
        yield spark
    except Exception as e:
        logger.error(f"Error occurred while starting spark session: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")




if __name__ == '__main__':
    logger = setup_logger('spark_job')
    args = arg_parser()
    job_name = args.job
    exit_code = 0
    with start_spark_session() as spark_session:
        try:
            run_job(job_name,spark_session)
            logger.info("Application completed successfully")
        except KeyboardInterrupt:
            logger.info(f"Job: {job_name} interrupted by the user. Exiting..")
            exit_code = 2
        except Exception as e:
            logger.error(f"Application failed: {e}")
            exit_code = 1
    logging.shutdown()
    sys.exit(exit_code)