from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
FLIGHTS_TABLE_SCHEMA = StructType([
        StructField("flight_id", IntegerType(), True),
        StructField("flight_no", StringType(), True),
        StructField("departure_station", StringType(), True),
        StructField("arrival_station", StringType(), True),
        StructField("departure_time_utc", LongType(), True),
        StructField("arrival_time_utc", LongType(), True)
       ])
CUSTOMERS_TABLE_SCHEMA = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", LongType(), True)
    ])
BOOKINGS_TABLE_SCHEMA = StructType([
        StructField("booking_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("flight_id", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True)
    ])

DEBEZIUM_SOURCE_SCHEMA = StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", StringType(), True)
    ])

def create_debezium_schema(table_schema_name):
    return StructType([StructField('before', table_schema_name, True),
                       StructField('after', table_schema_name, True),
                       StructField('source', DEBEZIUM_SOURCE_SCHEMA, True),
                       StructField('transaction', StringType(), True),
                       StructField('op', StringType(), True),
                       StructField('ts_ms', LongType(), True),
                       StructField('ts_us', LongType(), True),
                       StructField('ts_ns', LongType(), True)
                       ])
FLIGHTS_DEBEZIUM_SCHEMA = create_debezium_schema(FLIGHTS_TABLE_SCHEMA)
CUSTOMERS_DEBEZIUM_SCHEMA = create_debezium_schema(CUSTOMERS_TABLE_SCHEMA)
BOOKINGS_DEBEZIUM_SCHEMA = create_debezium_schema(BOOKINGS_TABLE_SCHEMA)
