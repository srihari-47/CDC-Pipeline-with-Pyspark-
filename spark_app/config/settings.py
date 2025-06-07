TOPIC={'flights':'flight_server.public.flights',
         'customers':'flight_server.public.customers',
         'bookings':'flight_server.public.bookings'}
KAFKA_BOOTSTRAP_SERVER='<tailscale ip of node running docker>:9092'
SPARK_MASTER='local'
SPARK_MASTER_PROD='spark://<tailscale-master-ip>:7077'
OUTPUT_PATH={'flights': 's3a://path/to/flights/',
             'customers':'s3a://path/to/customers/',
             'bookings':'s3a://path/to/bookings/'}
CHECKPOINT_PATH={'flights':'s3a://path/to/checkpoints/flights/',
             'customers':'s3a://path/to/checkpoints/customers/',
             'bookings':'s3a://path/to/checkpoints/bookings/'}
DEPLOY_MODE = 'client'
SPARK_APP_NAME = 'Spark_app'
SPARK_PACKAGE = ','.join(['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5',"org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"])
TIMEZONE = 'UTC'
MAX_RETRY_ATTEMPTS = 3


