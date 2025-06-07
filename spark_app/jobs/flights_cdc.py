from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,from_unixtime
from contextlib import contextmanager
from spark_app.config.settings import DEPLOY_MODE, SPARK_APP_NAME, SPARK_PACKAGE, TIMEZONE
from spark_app.utils.exceptions import JobExecutionError
from spark_app.utils.logger_config import setup_logger
from spark_app.config.schema import FLIGHTS_DEBEZIUM_SCHEMA
from spark_app.utils.CDCBaseClass import CDCBaseClass
import logging
import sys


class FlightCDCProcessor(CDCBaseClass):
    """
    Processes Change Data Capture (CDC) streams for flight-related data.

    This class inherits from CDCBaseClass and implements specific data transformation
    logic for processing flight data streams. The transformation involves filtering out
    null data, formatting required columns, and preparing the data for the sink.

    :ivar FLIGHTS_DEBEZIUM_SCHEMA: The schema structure used for deserializing the
        Debezium CDC data for flights.
    :type FLIGHTS_DEBEZIUM_SCHEMA: StructType
    """
    def transform_data(self, stream):
        try:
            micro_to_sec = 1000000
            milli_to_sec = 1000
            # Key is not needed, so we are taking in only the values. Since the messages are not deserialized, they are in binary form so converted to string.
            df_ = stream.select(from_json(col('value').cast('String'), FLIGHTS_DEBEZIUM_SCHEMA).alias('data'))

            # Filtering out the null data, we are formatting the other necessary columns and writing the same to sink.
            df_ = (df_.filter(col('data.op').isNotNull())
                  .select(col("data.op").alias("DB_OP"),
                          col('data.after.flight_id').alias('FLT_ID'),
                          col('data.after.flight_no').alias('FLT_NO'),
                          col('data.after.departure_station').alias('DEP_STN'),
                          col('data.after.arrival_station').alias('ARR_STN'),
                          from_unixtime(col('data.after.departure_time_utc') / micro_to_sec).alias('DEP_TIME_UTC'),
                          from_unixtime(col('data.after.arrival_time_utc') / micro_to_sec).alias('ARR_TIME_UTC'),
                          col('data.before').alias('FLT_DATA_BEFORE_UPD'),
                          from_unixtime(col('data.source.ts_ms') / milli_to_sec).alias('UPDATED_AT_UTC')))
            return df_
        except Exception as e:
            self.logger.error(f'Transformation could not be done due to {e}')
            self.logger.exception('Transformation failed with error stack trace:')
            raise JobExecutionError from e

def flight_cdc(spark):
    """
    Processes CDC (Change Data Capture) for flight data.

    This function creates an instance of the FlightCDCProcessor class, passing
    the provided Spark session. It then initiates the CDC process for the 'flights'
    dataset. This function is designed to handle CDC processing logic, enabling
    the system to track and manage incremental changes in flight data.

    :param spark: Spark session used for executing the process.

    """
    cdc_processor = FlightCDCProcessor(spark,'flights')
    cdc_processor.run()


@contextmanager
def start_spark_session() -> SparkSession:
    """
    Context manager to start and manage a Spark session.

    This context manager initializes a SparkSession with specified configurations,
    ensures proper handling of exceptions that may occur during the SparkSession's
    lifecycle, and stops the Spark session cleanly after use. The Spark session is
    yielded for operations within the context manager block. The function handles
    logging for successful initialization, errors during initialization, and
    session termination.

    :yield:
        Yields the created `SparkSession` object for use within the context block.
    :raises Exception:
        If an error occurs while initializing the Spark session, the exception
        is logged and raised.
    """
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .config("spark.submit.deployMode", DEPLOY_MODE) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.session.timeZone", TIMEZONE) \
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
    logger = setup_logger(__name__)
    exit_code = 0
    with start_spark_session() as spark_session:
        try:
            logger.info("Starting the CDC job for FLIGHTS table")
            flight_cdc(spark_session)
            logger.info("Completed the CDC job for FLIGHTS table")
        except KeyboardInterrupt:
            logger.info("User terminated the CDC job for FLIGHTS table execution. Exiting the application..")
            exit_code = 1
        except Exception as e:
            logger.error(f"Error occurred while running the CDC job for FLIGHTS table: {e}")
            exit_code = 2
    logging.shutdown()
    sys.exit(exit_code)









