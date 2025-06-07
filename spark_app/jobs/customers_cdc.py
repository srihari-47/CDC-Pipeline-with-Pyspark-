from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,from_unixtime
from contextlib import contextmanager
from spark_app.config.settings import DEPLOY_MODE, SPARK_APP_NAME, SPARK_PACKAGE, TIMEZONE
from spark_app.utils.exceptions import JobExecutionError
from spark_app.utils.logger_config import setup_logger
from spark_app.config.schema import CUSTOMERS_DEBEZIUM_SCHEMA
from spark_app.utils.CDCBaseClass import CDCBaseClass
import logging
import sys


class CustomerCDCProcessor(CDCBaseClass):
    """
    Processes Change Data Capture (CDC) streams for Customer-related data.

    This class inherits from CDCBaseClass and implements specific data transformation
    logic for processing Customer data streams. The transformation involves filtering out
    null data, formatting required columns, and preparing the data for the sink.

    """
    def transform_data(self, stream):
        try:
            micro_to_sec = 1000000
            milli_to_sec = 1000
            # Key is not needed, so we are taking in only the values. Since the messages are not deserialized, they are in binary form so converting them to string.
            df_ = stream.select(from_json(col('value').cast('String'), CUSTOMERS_DEBEZIUM_SCHEMA).alias('data'))

            # Filtering out the null data, we are formatting the other necessary columns and writing the same to sink.
            df_ = (df_.filter(col('data.op').isNotNull())
                   .select(col("data.op").alias("DB_OP"),
                           col('data.after.customer_id').alias('CUST_ID'),
                           col('data.after.first_name').alias('FIRST_NAME'),
                           col('data.after.middle_name').alias('MIDDLE_NAME'),
                           col('data.after.last_name').alias('LAST_NAME'),
                           col('data.after.full_name').alias('FULL_NAME'),
                           col('data.after.email').alias('EMAIL'),
                           from_unixtime(col('data.after.created_at') / micro_to_sec).alias('CREATED_AT_UTC'),
                           col('data.before').alias('CUST_DATA_BEFORE_UPD'),
                           from_unixtime(col('data.source.ts_ms') / milli_to_sec).alias('UPDATED_AT_UTC')))
            return df_
        except Exception as e:
            self.logger.error(f'Transformation could not be done due to {e}')
            self.logger.exception('Transformation failed with error stack trace:')
            raise JobExecutionError from e

def customer_cdc(spark):
    """
    Processes CDC (Change Data Capture) for customer data.

    This function creates an instance of the CustomerCDCProcessor class, passing
    the provided Spark session. It then initiates the CDC process for the 'Customers'
    dataset. This function is designed to handle CDC processing logic, enabling
    the system to track and manage incremental changes in Customer data.

    :param spark: Spark session used for executing the process.

    """
    cdc_processor = CustomerCDCProcessor(spark,'customers')
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
            logger.info("Starting the CDC job for CUSTOMERS table")
            customer_cdc(spark_session)
            logger.info("Completed the CDC job for CUSTOMERS table")
        except KeyboardInterrupt:
            logger.info("User terminated the CDC job for CUSTOMERS table execution. Exiting the application..")
            exit_code = 1
        except Exception as e:
            logger.error(f"Error occurred while running the CDC job for CUSTOMERS table: {e}")
            exit_code = 2
    logging.shutdown()
    sys.exit(exit_code)









