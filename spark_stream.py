import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_keyspace(session):
    ''' Create keyspace '''
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_stream
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
        logging.info('Keyspace created')
    except Exception as e:
        logging.error('Am error occured: %s', e)


def create_table(session):
    ''' Create table '''
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_stream.users_created (
                id UUID PRIMARY KEY,
                first_name text,
                last_name text,
                gender text,
                address text,
                pastcode text,
                email text,
                username text,
                dob text,
                register_date text,
                phone text,
                picture text
            )
        """)
        logging.info('Table created')
    except Exception as e:
        logging.error('Am error occured: %s', e)


def insert_data(session, **kwargs):
    logging.info('Inserting data')
    logging.info(kwargs)

    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    pastcode = kwargs.get('pastcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    register_date = kwargs.get('register_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_stream.users_created (id, first_name, last_name, gender, address, pastcode, email, username, dob, register_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, address, pastcode, email, username, dob, register_date, phone, picture))
        logging.info('Data inserted')
    except Exception as e:
        logging.error('Am error occured: %s', e)


def create_spark_connection():
    ''' Create spark connection '''
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("Spark Streaming") \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1', 'org.apache.spark:spark-streaming-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("WARN")
        logging.info('Spark connection created')
    except Exception as e:
        logging.error('Am error occured: %s', e)

    return s_conn


def create_cassandra_connection():
    ''' Create cassandra connection '''
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error('Am error occured: %s', e)
        return None


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info('Kafka connection created')
    except Exception as e:
        logging.error('Am error occured: %s', e)

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("pastcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("register_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        df = connect_to_kafka(spark_conn)
        selection_def = df.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)")
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
    # cassandra = create_cassandra_connection()
