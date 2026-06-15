import logging
import os
# from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

# Konfigurasi Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Suntikkan JAVA_HOME langsung ke environment runtime Python
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

os.environ.pop('SPARK_HOME', None)


def create_keyspace(session):
    """Membuat keyspace di Cassandra jika belum ada."""
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
    )
    logging.info("Keyspace 'spark_streams' created or already exists.")


def create_table(session):
    """Membuat tabel created_users jika belum ada."""
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name text,
            last_name text,
            gender text,
            address text,
            postcode text,
            email text,
            username text,
            dob text,
            registered_date text,
            phone text,
            picture text
        )          
    """
    )
    logging.info("Table 'created_users' created or already exists.")


def create_spark_connection():
    """Menginisialisasi dan mengembalikan Spark Session."""
    try:
        # Menyelaraskan versi Scala ke _2.12 untuk menghindari konflik dependensi
        spark_jars = (
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )

        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            # .master("local[*]")  # <--- TAMBAHKAN BARIS INI SECARA EKSPLISIT
            .config("spark.jars.packages", spark_jars)
            .config("spark.cassandra.connection.host", "cassandra")
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully.")
        return s_conn
    except Exception as e:
        logging.error(f"Failed to create Spark connection: {e}")
        return None


def connect_to_kafka(spark_conn):
    """Membaca stream data dari topik Kafka."""
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Connected to Kafka successfully.")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None


# def create_cassandra_connection():
#     """Menginisialisasi koneksi ke Cluster Cassandra."""
#     try:
#         cluster = Cluster(["localhost"])
#         cas_session = cluster.connect()
#         logging.info("Cassandra connection created successfully.")
#         return cas_session
#     except Exception as e:
#         logging.error(f"Failed to connect to Cassandra: {e}")
#         return None


def create_selection_df_from_kafka(spark_df):
    """Melakukan parsing data JSON dari Kafka menggunakan Schema terdefinisi."""
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("postcode", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    # Parsing nilai byte dari Kafka menjadi string, lalu ekstrak objek JSON-nya
    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return sel


if __name__ == "__main__":
    # 1. Inisialisasi Koneksi Spark
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # 2. Hubungkan ke Kafka dan dapatkan DataFrame awal
        df = connect_to_kafka(spark_conn)
        
        if df is not None:
            # Parsing JSON dari Kafka
            parsed_df = create_selection_df_from_kafka(df)

            # PERBAIKAN MUTAKHIR: Filter data agar record yang ID-nya NULL tidak bikin crash
            selection_df = parsed_df.filter(col("id").isNotNull())

            # 4. Tulis Stream Data langsung ke Cassandra
            logging.info("Streaming query is starting...")
            streaming_query = (
                selection_df.writeStream.format(
                    "org.apache.spark.sql.cassandra"
                )
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .option("checkpointLocation", "./checkpoint")
                .start()
            )

            streaming_query.awaitTermination()
        else:
            logging.error("Kafka DataFrame is None. Cannot proceed to selection.")
    else:
        logging.error("Spark connection failed. Exiting.")