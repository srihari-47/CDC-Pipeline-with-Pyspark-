import os
import time
import signal
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from spark_app.utils.exceptions import JobExecutionError
from spark_app.utils.logger_config import setup_logger
from abc import ABC, abstractmethod
from spark_app.config.settings import KAFKA_BOOTSTRAP_SERVER, TOPIC, MAX_RETRY_ATTEMPTS
from spark_app.config.settings import CHECKPOINT_PATH,OUTPUT_PATH


class CDCBaseClass(ABC):
    """
    Base class for managing streaming jobs using Spark.

    This class provides fundamental operations for streaming jobs, such as
    setting up signal handlers, handling streaming queries, and dealing with
    kafka stream data sources. It ensures proper handling of signals to gracefully
    terminate active queries and attempts multiple retries with backoff for robust
    stream connections.

    :ivar spark: Holds a reference to a SparkSession instance.
    :type spark: SparkSession
    :ivar logger: Logger instance for logging messages.
    :type logger: logging.logger
    :ivar query: Reference to the currently active StreamingQuery or None.
    :type query: Optional[StreamingQuery]
    """

    def __init__(self, spark: SparkSession,topic_name: str):
        self.spark = spark
        self.logger = setup_logger(self.__class__.__name__)
        self.query: Optional[StreamingQuery] = None
        self.topic_name = topic_name
        os.makedirs(os.path.join(CHECKPOINT_PATH[topic_name]), exist_ok=True)
        self.setup_signal_handler()

    def setup_signal_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Signal handler called with signal {signum}")
        if self.query and self.query.isActive:
            self.query.stop()

    def read_stream(self) -> DataFrame:
        """
        Reads a streaming DataFrame from a Kafka topic using Spark Structured Streaming.

        This method tries to read a stream from a specified Kafka topic.
        It uses the Kafka configurations, including the bootstrap server and
        topic name, to connect to the stream. The process includes retrying
        with exponential backoff strategy in case of failures until the maximum
        retry attempts are reached. If the stream cannot be successfully read
        after the allowed retries, an exception is raised.


        :return: The Kafka streaming DataFrame.
        :rtype: DataFrame

        :raises JobExecutionError: If the connection attempts exceed the maximum
            retries or another critical failure occurs during connecting to the
            stream.
        """
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                self.logger.info("Reading stream")
                stream = self.spark.readStream \
                    .format('kafka') \
                    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVER) \
                    .option('subscribe', TOPIC[self.topic_name]) \
                    .option('startingOffsets', 'earliest') \
                    .option("maxOffsetsPerTrigger", "10000") \
                    .option('failOnDataLoss', 'false') \
                    .load()
                self.logger.info("Successfully connected to stream.")
                return stream
            except Exception as e:
                self.logger.error(f"Error occurred while reading stream...retrying {attempt} of {MAX_RETRY_ATTEMPTS}")
                if attempt == MAX_RETRY_ATTEMPTS:
                    raise JobExecutionError(
                        f"Failed to connect to the stream after {MAX_RETRY_ATTEMPTS} retries") from e
                # Exponential backoff strategy
                back_off_time = min(2 ** attempt, 30)
                self.logger.info(f"Sleeping for {back_off_time} seconds before retrying...")
                time.sleep(back_off_time)
        raise JobExecutionError("Unexpected error happened while connecting to the stream")

    @abstractmethod
    def transform_data(self, stream: DataFrame) -> DataFrame:
        """
        Transforms the input data stream by applying specific transformation logic.

        This method is intended to be overridden by subclasses to implement
        custom transformation logic tailored to their specific requirements.
        The default implementation does nothing.

        :param self: Represents the instance of the class.
        :param stream: The input data stream to be transformed.
                       It is provided as a DataFrame object.
        :return: A DataFrame after applying the transformation logic.
        """
        pass

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        """
        Writes the provided streaming DataFrame to the console with a specified checkpoint
        location and configuration. The method ensures that the directory for the checkpoint
        location exists before initiating the streaming query. Streams are processed every 10
        seconds and use the 'append' output mode. The function returns a StreamingQuery
        object if the operation is successful.

        :param df: DataFrame
            The input streaming DataFrame that needs to be written to the console.
        :return: StreamingQuery
            The initiated streaming query object representing the continuous processing
            pipeline.
        """
        try:

            query = df.writeStream \
                .format('parquet') \
                .option('path',OUTPUT_PATH[self.topic_name]) \
                .option('checkpointLocation', CHECKPOINT_PATH[self.topic_name]) \
                .outputMode('append') \
                .trigger(processingTime='10 seconds') \
                .start()
            return query
        except Exception as e:
            self.logger.error(f"Error occurred while writing stream: {e}")
            raise JobExecutionError("Failed to write stream") from e

    def monitor_query(self, query: StreamingQuery):
        """
        Monitors the progress and status of a `StreamingQuery` to identify potential issues such as stalling.
        Logs information about the query's progress, including input rate, processing time, and batch ID.
        Handles various exceptions, including query errors, keyboard interruptions, and unexpected errors,
        ensuring the proper termination of the query. Monitors input rates and checks if the stream appears
        to be stalled based on a predefined threshold.

        :param self: The instance of the class in which the method is defined.
        :param query: The streaming query being monitored.
        :type query: StreamingQuery
        :return: None
        """
        try:
            last_progress_time = time.time()
            stall_threshold = 180
            while query.isActive:
                try:
                    status = query.status
                    progress = query.lastProgress
                    self.logger.info(f"Streaming query status: {status}")
                    if progress:
                        if progress.get('inputRowsPerSecond', 0) > 0:
                            last_progress_time = time.time()
                            self.logger.info(
                                f"Input rate: {progress.get('inputRowsPerSecond', 'N/A')}/sec, "
                                f"Processing rate: {progress.get('processingTimeDuration', 'N/A')}, "
                                f"Batch ID: {progress.get('batchId', 'N/A')}")
                            # Check for stalled stream
                            if time.time() - last_progress_time > stall_threshold:
                                self.logger.warning("Stream appears to be stalled. Consider investigation.")
                    time.sleep(5)
                except Exception as e:
                    self.logger.error(f"Error occurred while monitoring query: {e}")
                    time.sleep(2)
            if query.exception:
                self.logger.error(f"Streaming query exception: {query.exception}")
                raise query.exception()
        except KeyboardInterrupt:
            self.logger.info("Ctrl+M was issued.Stopping query...")
            query.stop()
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while monitoring query: {e}")
            query.stop()
            raise
        finally:
            self.logger.info("Waiting for stream termination...")
            query.awaitTermination()

    def run(self):
        """
        Executes the main logic of connecting to the data stream, performing transformations,
        writing the processed data to the sink, and monitoring the streaming query. Any errors
        that occur during the execution are handled appropriately, and resources are released
        in a controlled manner to ensure no active streams are left unclosed.

        :return: None
        :raises JobExecutionError: Raised when an error occurs specific to job execution.
        :raises Exception: Raised for any unexpected errors encountered during the run.
        """
        try:
            self.logger.info("Starting to connect to the stream")
            stream = self.read_stream()
            # applying transformation to the data stream
            transformed_df = self.transform_data(stream)
            # logging schema for verification
            transformed_df.printSchema()
            # writing the stream to the sink
            self.query = self.write_stream(transformed_df)
            # Monitoring the streaming query
            self.monitor_query(self.query)
            self.logger.info("Flight CDC processing completed successfully")
        except JobExecutionError as e:
            self.logger.error(f"Error occurred while running the CDC job: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while running the CDC job: {e}")
            raise
        finally:
            if self.query and self.query.isActive:
                self.query.stop()
                self.logger.info("Streaming query stopped.")





